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
                dt_atualizacao as dt_ultima_atualizacao, 
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
    
    colunas_data = ['dt_criacao', 'dt_inicio', 'dt_termino', 'dt_ultima_atualizacao']
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Define o status da OS com base nas datas de início e término
    df['status'] = 'Concluída' 
    df.loc[df['dt_inicio'].isna() & df['dt_termino'].isna(), 'status'] = 'Em aberto' # Aguardando Início
    df.loc[df['dt_inicio'].notna() & df['dt_termino'].isna(), 'status'] = 'Em andamento' # Ativa
    
    # Calcula o tempo que a OS está 'Em aberto' (aguardando início)
    df['tempo_em_aberto'] = np.nan
    mask_em_aberto_ou_iniciando = df['dt_inicio'].isna() & df['dt_criacao'].notna()
    # Calcula a diferença do momento atual para as OS ainda não iniciadas
    df.loc[mask_em_aberto_ou_iniciando, 'tempo_em_aberto'] = \
        (datetime.now() - df.loc[mask_em_aberto_ou_iniciando, 'dt_criacao']).dt.total_seconds() / (24*60*60)

    # Formata 'tempo_em_aberto' como string para exibição na tabela
    df['tempo_em_aberto_str'] = df['tempo_em_aberto'].apply(
        lambda x: f"{x:.2f} dias" if pd.notna(x) else "N/A"
    )
    
    return df

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
        
        /* Estilizando o dataframe (tabela de chamados) */
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
            background-color: #2a2e3a; /* Fundo do cabeçalho */
            color: #00CC96; /* Texto do cabeçalho */
            padding: 15px 20px;
            text-align: left;
            border-bottom: 3px solid #00CC96; /* Borda inferior mais grossa */
            font-size: 1.1em;
            font-weight: 700;
        }
        .stDataFrame td {
            background-color: #0E1117; /* Fundo das células de dados */
            color: #FAFAFA; /* Texto das células de dados */
            padding: 12px 20px;
            border-bottom: 1px solid #2a2e3a;
            font-size: 0.95em;
        }
        .stDataFrame tr:hover td {
            background-color: #1a1e26; /* Fundo ao passar o mouse */
        }
        /* Estilos para mensagens st.success e st.info */
        [data-testid="stSuccess"] {
            background-color: #00CC9620 !important; /* Verde claro com transparência */
            border-left: 8px solid #00CC96 !important;
            color: #00CC96 !important;
            font-weight: 600;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
        }
        [data-testid="stInfo"] {
            background-color: #FFA15A20 !important; /* Laranja com transparência */
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
        # Isso evita que o Streamlit adicione conteúdo novo a cada iteração sem limpar o anterior
        placeholder_content = st.empty()
        with placeholder_content.container():
            # --- Título Principal do Painel ---
            st.markdown('<div class="main-panel-title"><h1>Painel de Acompanhamento de OS</h1></div>', unsafe_allow_html=True)
            
            # --- Informação de Última Atualização ---
            current_time_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            st.markdown(f"<p class='last-updated'>Última atualização: {current_time_str}</p>", unsafe_allow_html=True)
            st.markdown("---") # Separador visual

            # --- Obtenção e Processamento de Dados ---
            with st.spinner("Carregando e processando dados do banco de dados..."):
                # A chave de atualização (time.time() // 30) força o Streamlit a re-executar
                # a função `obter_ordens_servico` a cada 30 segundos, ignorando o cache
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

            # Usando st.columns para distribuir as métricas
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
                
                # Selecionar e renomear colunas para exibição na tabela
                df_display_aberto = os_aguardando_inicio[[
                    'nr_os', 'ds_solicitacao', 'nm_solicitante', 'ie_prioridade', 'dt_criacao', 'nm_responsavel', 'tempo_em_aberto_str'
                ]].rename(columns={
                    'nr_os': 'Nº OS', 
                    'ds_solicitacao': 'Solicitação', 
                    'nm_solicitante': 'Solicitante',
                    'ie_prioridade': 'Prioridade', 
                    'dt_criacao': 'Criada Em',
                    'nm_responsavel': 'Responsável Designado',
                    'tempo_em_aberto_str': 'Tempo Aguardando' 
                })
                
                # Formatar a coluna 'Criada Em' para exibir apenas data e hora
                df_display_aberto['Criada Em'] = df_display_aberto['Criada Em'].dt.strftime('%d/%m/%Y %H:%M')
                
                # AQUI ESTÁ A CORREÇÃO FINAL: O nome da variável está correto agora
                st.dataframe(df_display_aberto) 
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
                # Maximo de 4 colunas para manter os cards visíveis em telas grandes
                num_cols_for_cards = min(4, num_responsaveis) 
                # Cria as colunas para os cards. Se não houver responsáveis, ainda cria 1 coluna vazia.
                cols_for_cards = st.columns(num_cols_for_cards if num_cols_for_cards > 0 else 1) 

                for idx, row in carga_por_responsavel.iterrows():
                    with cols_for_cards[idx % num_cols_for_cards]: 
                        # O st.info é estilizado via CSS para parecer um card bonito
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
