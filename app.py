import pandas as pd
import oracledb
import streamlit as st
from datetime import datetime, timedelta
import numpy as np
import time 

# --- Configuração da página do Streamlit ---
# Layout "wide" para ocupar a largura total e "collapsed" para esconder a sidebar, ideal para TV
st.set_page_config(
    page_title="Painel de Acompanhamento de OS - TV",
    page_icon="📺",
    layout="wide", 
    initial_sidebar_state="collapsed" 
)

# --- Configuração do Banco de Dados Oracle (Globais para fácil acesso) ---
USERNAME = 'TASY'
PASSWORD = 'aloisk'
HOST = '10.250.250.190'
PORT = 1521
SERVICE = 'dbprod.santacasapc'

# Inicializa o cliente Oracle Instant Client
try:
    oracledb.init_oracle_client()
except Exception as e:
    # Em um painel de TV, erros na sidebar não são ideais. Exibimos na tela principal.
    st.error(f"Erro na inicialização do Oracle Instant Client: {e}. Verifique a configuração e as variáveis de ambiente.")

# --- Funções de Conexão e Obtenção de Dados ---
def criar_conexao(username, password, host, port, service):
    """Cria e retorna uma nova conexão com o banco de dados Oracle."""
    try:
        # Tentativa 1: Usando DSN com formato padrão
        conn = oracledb.connect(user=username, password=password, 
                               dsn=f"{host}:{port}/{service}")
        return conn
    except Exception as e:
        # Erro de conexão exibido na tela principal do painel
        st.error(f"Erro ao tentar conectar ao banco de dados: {e}. Verifique as credenciais e a conexão com o servidor.")
        return None

# decorated com st.cache para otimização, com refresh_key para forçar atualização a cada 30s
@st.cache(allow_output_mutation=True, suppress_st_warning=True)
def obter_ordens_servico(username, password, host, port, service, refresh_key): 
    """Obtém os dados das ordens de serviço do grupo de trabalho 12, criando uma nova conexão."""
    conn = None 
    try:
        conn = criar_conexao(username, password, host, port, service)
        if conn is None:
            return pd.DataFrame() # Retorna DataFrame vazio se a conexão falhar

        query = """
        select  nr_sequencia as nr_os, 
                ds_dano_breve as ds_solicitacao, 
                obter_nome_pf(cd_pessoa_solicitante) as nm_solicitante, 
                ie_prioridade,
                dt_ordem_servico as dt_criacao, 
                dt_inicio_real as dt_inicio, 
                dt_fim_real as dt_termino, 
                nm_usuario as nm_responsavel, 
                ds_dano as ds_completa_servico
        from    MAN_ORDEM_SERVICO 
        where   NR_GRUPO_TRABALHO = 12
        """
        
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta SQL: {e}. Verifique a query ou o acesso ao banco de dados.")
        return pd.DataFrame()
    finally:
        if conn: # Garante que a conexão seja fechada se foi aberta
            try:
                conn.close()
            except Exception as e:
                st.warning(f"Aviso: Erro ao fechar a conexão do banco de dados: {e}")

# --- Funções de Processamento de Dados ---
def processar_dados(df):
    """Processa e enriquece os dados para análise e visualização."""
    if df.empty:
        return df

    df.columns = [col.lower() for col in df.columns]
    
    colunas_data = ['dt_criacao', 'dt_inicio', 'dt_termino']
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Define o status da OS com base nas datas de início e término
    df['status'] = 'Concluída' 
    df.loc[df['dt_inicio'].isna() & df['dt_termino'].isna(), 'status'] = 'Em aberto' # Aguardando Início
    df.loc[df['dt_inicio'].notna() & df['dt_termino'].isna(), 'status'] = 'Em andamento' # Ativa
    
    # Calcula o tempo que a OS está 'Em aberto' (aguardando início) em dias (float)
    df['tempo_em_aberto_dias'] = np.nan
    mask_em_aberto_ou_iniciando = df['dt_inicio'].isna() & df['dt_criacao'].notna()
    # Calcula a diferença do momento atual para as OS ainda não iniciadas
    df.loc[mask_em_aberto_ou_iniciando, 'tempo_em_aberto_dias'] = \
        (datetime.now() - df.loc[mask_em_aberto_ou_iniciando, 'dt_criacao']).dt.total_seconds() / (24*60*60)

    # Não vamos criar 'tempo_em_aberto_str' aqui, faremos a formatação direto no HTML
    
    return df

# --- Função para gerar a tabela de OS Abertas com HTML customizado ---
def generate_styled_open_os_table(df_open_os):
    if df_open_os.empty:
        return ""

    html_string = """
    <table class="custom-open-os-table">
        <thead>
            <tr>
                <th>Nº OS</th>
                <th>Solicitação</th>
                <th>Solicitante</th>
                <th>Prioridade</th>
                <th>Criada Em</th>
                <th>Responsável Designado</th>
                <th>Tempo Aguardando</th>
            </tr>
        </thead>
        <tbody>
    """

    for index, row in df_open_os.iterrows():
        # Determine a classe da linha baseada no tempo aguardando
        row_class = ""
        if pd.notna(row['tempo_em_aberto_dias']):
            if row['tempo_em_aberto_dias'] >= 5: # Mais de 5 dias
                row_class = "danger-row"
            elif row['tempo_em_aberto_dias'] >= 2: # Entre 2 e 5 dias
                row_class = "warning-row"
            elif row['tempo_em_aberto_dias'] >= 0.5: # Entre 0.5 e 2 dias
                row_class = "info-row"
            else: # Menos de 0.5 dias (12 horas)
                row_class = "success-row"
        
        tempo_aguardando_display = f"{row['tempo_em_aberto_dias']:.2f} dias" if pd.notna(row['tempo_em_aberto_dias']) else "N/A"
        criada_em_display = row['dt_criacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['dt_criacao']) else "N/A"
        
        html_string += f"""
            <tr class="{row_class}">
                <td>{row['nr_os']}</td>
                <td>{row['ds_solicitacao']}</td>
                <td>{row['nm_solicitante']}</td>
                <td>{row['ie_prioridade']}</td>
                <td>{criada_em_display}</td>
                <td>{row['nm_responsavel'] if pd.notna(row['nm_responsavel']) else 'Não Atribuído'}</td>
                <td>{tempo_aguardando_display}</td>
            </tr>
        """
    html_string += "</tbody></table>"
    return html_string

# --- Função Principal do Aplicativo Streamlit ---
def main():
    # Injeta CSS personalizado para estilização do painel (Onde a magia acontece)
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700;800&display=swap');

        /* Estilo base para o corpo da aplicação */
        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Montserrat', sans-serif;
            background-color: #0E1117; /* Fundo escuro */
            color: #FAFAFA; /* Texto claro */
        }

        /* Estilo para títulos h1, h2 */
        h1, h2 {
            color: #00CC96; /* Verde vibrante para títulos */
            font-weight: 800; /* Mais negrito */
            text-shadow: 2px 2px 5px rgba(0, 0, 0, 0.4); /* Sombra para destaque */
            margin-bottom: 25px;
        }
        h2 {
            font-size: 2.2em;
        }

        /* Estilizando os cards de métricas (st.metric) */
        [data-testid="stMetric"] {
            background-color: #1a1e26; /* Fundo ligeiramente mais claro */
            padding: 20px;
            border-radius: 12px; /* Cantos mais arredondados */
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.5); /* Sombra mais proeminente */
            border: 1px solid #2a2e3a; /* Borda sutil */
            text-align: center;
            margin-bottom: 25px;
            transition: transform 0.2s ease-in-out; /* Efeito hover */
        }
        [data-testid="stMetric"]:hover {
            transform: translateY(-5px); /* Levanta o card no hover */
        }
        [data-testid="stMetricValue"] {
            font-size: 3.5em !important; /* Tamanho do valor */
            color: #00CC96 !important; /* Cor do valor */
            font-weight: 800;
        }
        [data-testid="stMetricLabel"] {
            font-size: 1.3em !important; /* Tamanho do label */
            color: #90929A !important; /* Cor mais suave para o label */
            font-weight: 600;
            text-transform: uppercase; /* Letras maiúsculas para o label */
        }
        [data-testid="stMetricDelta"] {
            font-size: 1.1em !important;
        }

        /* Estilizando os cards de carga de trabalho (st.info é usado para isso) */
        [data-testid="stAlert"] {
            background-color: #1a1e26 !important; 
            color: #FAFAFA !important; 
            border: 1px solid #00CC96 !important; 
            border-left: 10px solid #00CC96 !important; /* Borda lateral grossa para destaque */
            border-radius: 10px !important;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.6) !important; /* Sombra mais proeminente */
            padding: 20px !important;
            margin-bottom: 20px !important;
            transition: transform 0.2s ease-in-out; /* Efeito hover */
        }
        [data-testid="stAlert"]:hover {
            transform: translateY(-3px); /* Levanta o card no hover */
        }
        [data-testid="stAlert"] .st-bv { /* Título do alert (Responsável) */
            font-size: 1.6em;
            font-weight: 700;
            color: #00CC96; /* Verde para o nome do responsável */
            margin-bottom: 10px;
        }
        [data-testid="stAlert"] p { /* Parágrafos dentro do alert (Qtd OS Ativas) */
            font-size: 1.2em;
            margin: 5px 0;
            font-weight: 600;
        }
        [data-testid="stAlert"] p strong { /* Negrito no número de OS Ativas */
            color: #FFA15A; /* Laranja para o número */
            font-size: 1.5em; /* Aumenta o tamanho do número */
        }
        
        /* Estilizando o dataframe genérico (se usado em outras seções) */
        .stDataFrame {
            border: 1px solid #2a2e3a;
            border-radius: 12px;
            overflow: hidden; 
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.5);
        }
        .stDataFrame table {
            width: 100%;
            border-collapse: collapse;
        }
        .stDataFrame th {
            background-color: #2a2e3a; 
            color: #00CC96; 
            padding: 15px 20px;
            text-align: left;
            border-bottom: 3px solid #00CC96; 
            font-size: 1.1em;
            font-weight: 700;
        }
        .stDataFrame td {
            background-color: #0E1117; 
            color: #FAFAFA; 
            padding: 12px 20px;
            border-bottom: 1px solid #2a2e3a;
            font-size: 0.95em;
        }
        .stDataFrame tr:hover td {
            background-color: #1a1e26; 
        }

        /* --- Estilos para a nova Tabela de OS Abertas (HTML customizado) --- */
        .custom-open-os-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            font-size: 0.95em;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.5);
        }
        .custom-open-os-table thead th {
            background-color: #2a2e3a;
            color: #00CC96;
            padding: 15px 20px;
            text-align: left;
            border-bottom: 3px solid #00CC96;
            font-weight: 700;
            font-size: 1.1em;
        }
        .custom-open-os-table tbody tr {
            transition: background-color 0.2s ease;
        }
        .custom-open-os-table tbody tr:hover {
            background-color: #1a1e26;
        }
        .custom-open-os-table tbody td {
            background-color: #0E1117;
            color: #FAFAFA;
            padding: 12px 20px;
            border-bottom: 1px solid #2a2e3a;
            vertical-align: middle;
        }

        /* Cores condicionais para as linhas da tabela de OS Abertas */
        .custom-open-os-table .success-row { /* Menos de 0.5 dias (Verde claro) */
            background-color: #00CC9615 !important; /* 15% de opacidade */
            border-left: 5px solid #00CC96;
        }
        .custom-open-os-table .info-row {    /* Entre 0.5 e 2 dias (Azul claro) */
            background-color: #1E90FF15 !important; 
            border-left: 5px solid #1E90FF;
        }
        .custom-open-os-table .warning-row { /* Entre 2 e 5 dias (Amarelo/Laranja) */
            background-color: #FFA15A15 !important; 
            border-left: 5px solid #FFA15A;
        }
        .custom-open-os-table .danger-row {  /* Mais de 5 dias (Vermelho) */
            background-color: #EF553B15 !important; 
            border-left: 5px solid #EF553B;
        }

        /* Estilos para mensagens st.success e st.info */
        [data-testid="stSuccess"] {
            background-color: #00CC9620 !important; 
            border-left: 8px solid #00CC96 !important;
            color: #00CC96 !important;
            font-weight: 600;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
        }
        [data-testid="stInfo"] {
            background-color: #FFA15A20 !important; 
            border-left: 8px solid #FFA15A !important;
            color: #FFA15A !important;
            font-weight: 600;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
        }

        /* Estilo para o cabeçalho principal do painel */
        .main-panel-title {
            text-align: center;
            padding: 30px 0;
        }
        .main-panel-title h1 {
            font-size: 4em;
            letter-spacing: 2px;
        }
        
        /* Estilo para o timestamp de atualização */
        .last-updated {
            text-align: right;
            color: #90929A;
            font-size: 1em;
            margin-bottom: 20px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # O loop infinito para auto-atualização do dashboard
    while True: 
        # Usamos st.empty() para "limpar" o conteúdo anterior e redesenhá-lo completamente
        placeholder_content = st.empty()
        with placeholder_content.container():
            # --- Título Principal do Painel ---
            st.markdown('<div class="main-panel-title"><h1>Painel de Acompanhamento de OS</h1></div>', unsafe_allow_html=True)
            
            # --- Adições para Depuração ---
            st.markdown("<h2>--- Início da Área de Depuração ---</h2>", unsafe_allow_html=True)
            st.write(f"Streamlit Version: {st.__version__}")
            st.markdown("<h3>Teste de HTML: Se você está vendo este texto, o HTML está sendo renderizado corretamente!</h3>", unsafe_allow_html=True)
            st.markdown("<p style='color: yellow; font-size: 20px;'>E este texto deve ser amarelo e grande. Se ambos aparecerem formatados, `unsafe_allow_html` funciona.</p>", unsafe_allow_html=True)
            st.markdown("<h2>--- Fim da Área de Depuração ---</h2>", unsafe_allow_html=True)
            st.markdown("---") # Separador visual


            # --- Informação de Última Atualização ---
            current_time_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            st.markdown(f"<p class='last-updated'>Última atualização: {current_time_str}</p>", unsafe_allow_html=True)
            st.markdown("---") # Separador visual

            # --- Obtenção e Processamento de Dados ---
            with st.spinner("Carregando e processando dados do banco de dados..."):
                df_raw = obter_ordens_servico(USERNAME, PASSWORD, HOST, PORT, SERVICE, time.time() // 30) 
                
            if df_raw.empty:
                st.error("Não foi possível carregar os dados das Ordens de Serviço. Verifique a conexão com o banco de dados e as configurações.")
                time.sleep(30) 
                continue 

            df_processed = processar_dados(df_raw)
            
            # --- Resumo Geral de Métricas (Cards no topo) ---
            st.markdown("<h2>Resumo Operacional</h2>", unsafe_allow_html=True)
            
            total_os_abertas = len(df_processed[df_processed['status'] == 'Em aberto'])
            total_os_em_andamento = len(df_processed[df_processed['status'] == 'Em andamento'])
            total_os_concluidas = len(df_processed[df_processed['status'] == 'Concluída'])
            total_geral_os = len(df_processed)

            col_met1, col_met2, col_met3, col_met4 = st.columns(4)
            with col_met1:
                st.metric(label="Total de OS", value=total_geral_os)
            with col_met2:
                st.metric(label="OS Concluídas", value=total_os_concluidas)
            with col_met3:
                st.metric(label="OS Em Andamento", value=total_os_em_andamento)
            with col_met4:
                st.metric(label="OS Aguardando Início", value=total_os_abertas)

            st.markdown("---") # Separador visual

            # --- Seção de Ordens de Serviço Abertas e Aguardando Início ---
            st.markdown("<h2>Ordens de Serviço Abertas e Aguardando Início</h2>", unsafe_allow_html=True)
            
            os_aguardando_inicio = df_processed[
                df_processed["status"] == "Em aberto" 
            ].copy() 

            os_aguardando_inicio = os_aguardando_inicio.sort_values(by="dt_criacao", ascending=True)

            if not os_aguardando_inicio.empty:
                st.success(f"**{len(os_aguardando_inicio)}** Ordens de Serviço atualmente aguardando início. Atenção às mais antigas!")
                
                # Gera a tabela HTML customizada com a nova função
                styled_table_html = generate_styled_open_os_table(os_aguardando_inicio)
                
                # --- LINHA REMOVIDA: Era a linha que exibia o HTML como texto ---
                # st.info("Verificando a string HTML gerada para a tabela...")
                # st.markdown(f"HTML da Tabela Gerado (primeiros 500 caracteres):\n```html\n{styled_table_html[:500]}\n```", unsafe_allow_html=True)
                # st.markdown("---")

                # Renderiza a tabela HTML
                st.markdown(styled_table_html, unsafe_allow_html=True) 
            else:
                st.info("🎉 Parabéns! Nenhuma Ordem de Serviço aguardando início no momento. Produtividade máxima!")
            
            st.markdown("---") # Separador visual

            # --- Seção de Carga de Trabalho por Responsável ---
            st.markdown("<h2>Carga de Trabalho de Ordens de Serviço Ativas por Responsável</h2>", unsafe_allow_html=True)
            
            os_em_andamento_ativas = df_processed[
                (df_processed["status"] == "Em andamento") & 
                (df_processed["nm_responsavel"].notna()) 
            ].copy()
            
            if not os_em_andamento_ativas.empty:
                carga_por_responsavel = os_em_andamento_ativas["nm_responsavel"].value_counts().reset_index()
                carga_por_responsavel.columns = ["Responsável", "OS Ativas"]
                
                num_responsaveis = len(carga_por_responsavel)
                num_cols_for_cards = min(4, num_responsaveis) 
                cols_for_cards = st.columns(num_cols_for_cards if num_cols_for_cards > 0 else 1) 

                for idx, row in carga_por_responsavel.iterrows():
                    with cols_for_cards[idx % num_cols_for_cards]: 
                        st.info( 
                            f"**{row['Responsável']}**\n\n"
                            f"**{int(row['OS Ativas'])}** OS Ativas"
                        )
            else:
                st.info("Nenhuma Ordem de Serviço ativa atribuída a um responsável no momento. Todos prontos para mais tarefas!")
            
            st.markdown("---") # Separador visual final

        # Pausa o script por 30 segundos antes da próxima re-execução completa
        time.sleep(30) 

# Ponto de entrada da aplicação Streamlit
if __name__ == "__main__":
    main()
