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

# --- Função para gerar os cards de OS Abertas com HTML customizado ---
def generate_open_os_cards(df_open_os):
    if df_open_os.empty:
        return ""

    html_cards = ""
    for index, row in df_open_os.iterrows():
        # Determine a classe do card baseada no tempo aguardando
        card_class = "os-card " # Base class
        if pd.notna(row['tempo_em_aberto_dias']):
            if row['tempo_em_aberto_dias'] >= 5: # Mais de 5 dias
                card_class += "os-card-danger"
            elif row['tempo_em_aberto_dias'] >= 2: # Entre 2 e 5 dias
                card_class += "os-card-warning"
            elif row['tempo_em_aberto_dias'] >= 0.5: # Entre 0.5 e 2 dias
                card_class += "os-card-info"
            else: # Menos de 0.5 dias (12 horas)
                card_class += "os-card-success"
        else: # Se não há tempo_em_aberto_dias (e.g., data de criação nula)
            card_class += "os-card-default" 

        tempo_aguardando_display = f"**{row['tempo_em_aberto_dias']:.2f} dias**" if pd.notna(row['tempo_em_aberto_dias']) else "N/A"
        criada_em_display = row['dt_criacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['dt_criacao']) else "N/A"
        
        html_cards += f"""
        <div class="{card_class}">
            <div class="os-card-header">
                <span class="os-card-id">OS #{row['nr_os']}</span>
                <span class="os-card-priority">Prioridade: {row['ie_prioridade']}</span>
            </div>
            <div class="os-card-body">
                <p class="os-card-solicitation">{row['ds_solicitacao']}</p>
                <div class="os-card-details">
                    <span class="os-card-info">Solicitante: {row['nm_solicitante']}</span>
                    <span class="os-card-info">Criada em: {criada_em_display}</span>
                    <span class="os-card-info">Responsável: {row['nm_responsavel'] if pd.notna(row['nm_responsavel']) else 'Não Atribuído'}</span>
                </div>
            </div>
            <div class="os-card-footer">
                <span>Aguardando há: {tempo_aguardando_display}</span>
            </div>
        </div>
        """
    return html_cards


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
        .main-panel-title h1 { /* Título principal */
            font-size: 2.5em; /* <--- Ajuste aqui: Título principal ainda menor */
            letter-spacing: 1px; /* Ajuste para não ficar muito espaçado */
            color: #00CC96;
            font-weight: 800;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.4); /* Sombra mais suave */
            margin-bottom: 15px; /* Margem inferior reduzida */
        }
        h2 { /* Títulos de seção */
            font-size: 1.5em; /* <--- Ajuste aqui: Títulos de seção ainda menores */
            color: #00CC96;
            font-weight: 800;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.4); /* Sombra mais suave */
            margin-bottom: 15px; /* Margem inferior reduzida */
        }

        /* Estilizando os cards de métricas (st.metric) */
        [data-testid="stMetric"] {
            background-color: #1a1e26;
            padding: 8px; /* <--- Ajuste aqui: Padding ainda mais reduzido */
            border-radius: 10px; /* Levemente menos arredondado */
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.4); /* Sombra mais suave */
            border: 1px solid #2a2e3a;
            text-align: center;
            margin-bottom: 8px; /* <--- Ajuste aqui: Margem inferior reduzida */
            transition: transform 0.2s ease-in-out;
        }
        [data-testid="stMetric"]:hover {
            transform: translateY(-3px); /* Efeito hover mais sutil */
        }
        [data-testid="stMetricValue"] {
            font-size: 2em !important; /* <--- Ajuste aqui: Tamanho do valor ainda mais reduzido */
            color: #00CC96 !important;
            font-weight: 800;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.9em !important; /* <--- Ajuste aqui: Tamanho do label ainda mais reduzido */
            color: #90929A !important;
            font-weight: 600;
            text-transform: uppercase;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.9em !important; /* <--- Ajuste aqui */
        }

        /* Estilizando o container para os cards de responsáveis (FORÇANDO UMA LINHA) */
        .responsavel-cards-container {
            display: flex;
            flex-wrap: nowrap; /* <--- CHAVE: Força todos os cards a ficarem na mesma linha */
            gap: 10px; /* <--- Ajuste aqui: Espaçamento entre os cards */
            justify-content: flex-start;
            margin-bottom: 20px;
            overflow-x: auto; /* Adiciona rolagem horizontal se não couberem */
            padding-bottom: 5px; /* Espaço para a barra de rolagem */
            scrollbar-width: thin; /* Firefox */
            scrollbar-color: #00CC96 #1a1e26; /* Firefox */
        }
        /* Estilo da barra de rolagem para Webkit (Chrome, Safari) */
        .responsavel-cards-container::-webkit-scrollbar {
            height: 8px;
        }
        .responsavel-cards-container::-webkit-scrollbar-track {
            background: #1a1e26;
            border-radius: 10px;
        }
        .responsavel-cards-container::-webkit-scrollbar-thumb {
            background-color: #00CC96;
            border-radius: 10px;
            border: 2px solid #1a1e26;
        }


        /* Estilizando os cards de carga de trabalho (st.info é usado para isso) */
        [data-testid="stAlert"] {
            flex-shrink: 0; /* Impede que os cards encolham */
            width: 180px; /* <--- Ajuste aqui: Largura fixa para cada card de responsável */
            background-color: #1a1e26 !important; 
            color: #FAFAFA !important; 
            border: 1px solid #00CC96 !important; 
            border-left: 8px solid #00CC96 !important; /* Borda um pouco menor */
            border-radius: 8px !important; /* Levemente menos arredondado */
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5) !important; /* Sombra mais suave */
            padding: 10px !important; /* Reduzido */
            margin-bottom: 0px !important; /* Removido para usar gap no container */
            transition: transform 0.2s ease-in-out;
        }
        [data-testid="stAlert"]:hover {
            transform: translateY(-2px); /* Efeito hover mais sutil */
        }
        [data-testid="stAlert"] .st-bv { 
            font-size: 1.2em; /* Reduzido */
            font-weight: 700;
            color: #00CC96;
            margin-bottom: 5px; /* Reduzido */
        }
        [data-testid="stAlert"] p { 
            font-size: 1em; /* Reduzido */
            margin: 2px 0; /* Reduzido */
            font-weight: 600;
        }
        [data-testid="stAlert"] p strong { 
            color: #FFA15A;
            font-size: 1.2em; /* Reduzido */
        }
        
        /* Estilizando o dataframe (tabela de chamados) - Streamlit Nativo */
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

        /* --- Estilos para os Cards de OS Abertas --- */
        .os-card {
            background-color: #1a1e26;
            border-radius: 8px;
            margin-bottom: 6px;
            padding: 2px 10px; 
            box-shadow: 0 3px 8px rgba(0, 0, 0, 0.3);
            transition: transform 0.2s ease-in-out;
            border-left: 6px solid transparent;
        }
        .os-card:hover {
            transform: translateY(-2px);
        }

        .os-card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1px;
        }
        .os-card-id {
            font-size: 1.1em;
            font-weight: 700;
            color: #00CC96;
        }
        .os-card-priority {
            font-size: 0.8em;
            font-weight: 600;
            color: #90929A;
            background-color: #2a2e3a;
            padding: 2px 4px;
            border-radius: 4px;
        }
        .os-card-solicitation {
            font-size: 1em;
            font-weight: 600;
            color: #FAFAFA;
            margin-bottom: 2px;
            line-height: 1.1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .os-card-details {
            display: flex;
            flex-wrap: wrap;
            gap: 5px;
            font-size: 0.75em;
            color: #90929A;
            margin-bottom: 2px;
        }
        .os-card-info {
            white-space: nowrap;
        }
        .os-card-footer {
            text-align: right;
            font-size: 0.9em;
            font-weight: 600;
            color: #FFA15A;
        }

        /* Cores condicionais para os cards de OS Abertas */
        .os-card-success { 
            background-color: #00CC9608 !important; 
            border-left-color: #00CC96 !important;
        }
        .os-card-info {    
            background-color: #1E90FF08 !important; 
            border-left-color: #1E90FF !important;
        }
        .os-card-warning { 
            background-color: #FFA15A08 !important; 
            border-left-color: #FFA15A !important;
        }
        .os-card-danger {  
            background-color: #EF553B08 !important; 
            border-left-color: #EF553B !important;
        }
        .os-card-default {
            background-color: #90929A08 !important;
            border-left-color: #90929A !important;
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
        # ATENÇÃO: Todo o conteúdo que deve ser atualizado/limpo a cada iteração DEVE estar dentro deste 'with'
        # Se você notar conteúdo duplicado ou não sendo limpo, verifique se está fora deste bloco.
        placeholder_content = st.empty()
        with placeholder_content.container():
            # --- Título Principal do Painel ---
            st.markdown('<div class="main-panel-title"><h1>Painel de Acompanhamento de OS</h1></div>', unsafe_allow_html=True)
            
            # --- Informação de Versão do Streamlit ---
            st.markdown(f"<p style='color: #90929A; text-align: center; font-size: 0.8em;'>Streamlit Version: {st.__version__}</p>", unsafe_allow_html=True)

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

            # --- Seção de Ordens de Serviço Abertas e Aguardando Início (Cards) ---
            st.markdown("<h2>Ordens de Serviço Abertas e Aguardando Início</h2>", unsafe_allow_html=True)
            
            os_aguardando_inicio = df_processed[
                df_processed["status"] == "Em aberto" 
            ].copy() 

            os_aguardando_inicio = os_aguardando_inicio.sort_values(by="dt_criacao", ascending=True)

            if not os_aguardando_inicio.empty:
                st.success(f"**{len(os_aguardando_inicio)}** Ordens de Serviço atualmente aguardando início. Atenção às mais antigas!")
                # Lembrete: Esta lista JÁ EXCLUI OS que estão 'Em andamento'.
                # A lógica de status é: 'Em aberto' = aguardando início; 'Em andamento' = já iniciada.
                
                # Gera e renderiza os cards HTML personalizados
                os_cards_html = generate_open_os_cards(os_aguardando_inicio)
                st.markdown(os_cards_html, unsafe_allow_html=True) 
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
                
                # Container para os cards de responsáveis
                st.markdown('<div class="responsavel-cards-container">', unsafe_allow_html=True)
                for idx, row in carga_por_responsavel.iterrows():
                    # Os st.info serão os cards que ficarão lado a lado
                    st.info( 
                        f"**{row['Responsável']}**\n\n"
                        f"**{int(row['OS Ativas'])}** OS Ativas"
                    )
                st.markdown('</div>', unsafe_allow_html=True) # Fecha o container
            else:
                st.info("Nenhuma Ordem de Serviço ativa atribuída a um responsável no momento. Todos prontos para mais tarefas!")
            
            st.markdown("---") # Separador visual final

        # Pausa o script por 30 segundos antes da próxima re-execução completa
        time.sleep(30) 

# Ponto de entrada da aplicação Streamlit
if __name__ == "__main__":
    main()
