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
    page_icon="��", # Ícone de TV para a página
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

@st.cache_data(ttl=30) # Atualiza os dados a cada 30 segundos
def obter_ordens_servico(username, password, host, port, service):
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

# --- Função para gerar os cards de Detalhes de OS Ativas/Concluídas com HTML customizado ---
def generate_os_details_cards(df, card_type):
    """Gera cards HTML para exibir detalhes de ordens de serviço ativas ou concluídas."""
    if df.empty:
        return ""

    html_cards = ""
    for index, row in df.iterrows():
        card_class = "os-card "
        status_text = ""
        dt_specific_label = ""
        dt_specific_value = "N/A"

        # Define o tipo de card e as informações específicas de data/status
        if card_type == "active":
            card_class += "os-card-info" # Cor azul para OS ativas
            status_text = "Em Andamento"
            dt_specific_label = "Iniciada em"
            dt_specific_value = row['dt_inicio'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['dt_inicio']) else "N/A"
        elif card_type == "completed":
            card_class += "os-card-success" # Cor verde para OS concluídas
            status_text = "Concluída"
            dt_specific_label = "Finalizada em"
            dt_specific_value = row['dt_termino'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['dt_termino']) else "N/A"
        else:
            card_class += "os-card-default" # Padrão, se nenhum tipo for especificado
            status_text = "Status Desconhecido"

        criada_em_display = row['dt_criacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(row['dt_criacao']) else "N/A"

        # Construção do HTML para cada card de OS
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
                    <span class="os-card-info">{dt_specific_label}: {dt_specific_value}</span>
                </div>
            </div>
            <div class="os-card-footer">
                <span>Status: {status_text}</span>
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

        /* Oculta o cabeçalho principal do Streamlit (onde fica o menu hambúrguer) */
        header[data-testid="stHeader"] {
            display: none !important;
        }

        /* Oculta o botão de menu/configurações (hambúrguer) */
        div[data-testid="stToolbar"] {
            display: none !important;
        }

        /* Oculta o rodapé "Made with Streamlit" */
        footer {
            display: none !important;
        }

        /* Oculta a sidebar, caso ela fosse visível em algum momento. */
        section[data-testid="stSidebar"] {
            display: none !important;
        }

        /* Garante que o conteúdo principal ocupe a largura total disponível */
        .block-container {
            padding-top: 0rem !important; /* Remove qualquer padding superior padrão */
            padding-left: 0rem !important; /* Remove padding lateral esquerdo */
            padding-right: 0rem !important; /* Remove padding lateral direito */
            padding-bottom: 0rem !important; /* Remove padding inferior */
            margin: 0 !important; /* Remove margens */
            max-width: 100% !important; /* Garante que o conteúdo ocupe 100% da largura */
        }

        /* Estilo base para o corpo da aplicação */
        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Montserrat', sans-serif;
            background-color: #0E1117; /* Fundo escuro */
            color: #FAFAFA; /* Texto claro */
        }

        /* Estilo para títulos h1, h2 */
        .main-panel-title h1 { /* Título principal */
            font-size: 2em;
            letter-spacing: 1px;
            color: #00CC96;
            font-weight: 800;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.4);
            margin-bottom: 10px;
        }
        h2 { /* Títulos de seção */
            font-size: 1.3em;
            color: #00CC96;
            font-weight: 800;
            text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.4);
            margin-bottom: 10px;
        }
        h3 { /* Subtítulos para detalhes */
            font-size: 1.1em;
            color: #FFA15A;
            font-weight: 700;
            margin-top: 15px;
            margin-bottom: 5px;
        }


        /* Estilizando os cards de métricas (st.metric) */
        [data-testid="stMetric"] {
            background-color: #1a1e26;
            padding: 5px;
            border-radius: 8px;
            box-shadow: 0 3px 8px rgba(0, 0, 0, 0.3);
            border: 1px solid #2a2e3a;
            text-align: center;
            margin-bottom: 5px;
            transition: transform 0.2s ease-in-out;
        }
        [data-testid="stMetric"]:hover {
            transform: translateY(-2px);
        }
        [data-testid="stMetricValue"] {
            font-size: 1.8em !important;
            color: #00CC96 !important;
            font-weight: 800;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.8em !important;
            color: #90929A !important;
            font-weight: 600;
            text-transform: uppercase;
        }
        [data-testid="stMetricDelta"] {
            font-size: 0.8em !important;
        }

        /* Estilo para o display visual do card (não o botão) */
        .workload-card-display {
            background-color: #1a1e26;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 3px 8px rgba(0, 0, 0, 0.3);
            border: 1px solid #2a2e3a;
            margin-bottom: 5px; /* Espaço entre o card e o botão */
            height: 100%; /* Ensure consistent height in columns */
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        .workload-card-display h4 { /* Responsible Name */
            font-size: 1.1em;
            font-weight: 700;
            color: #00CC96;
            margin-bottom: 5px;
            text-align: center;
        }
        .workload-card-display p { /* Metric Text */
            font-size: 0.9em;
            margin: 2px 0;
            font-weight: 600;
            text-align: center;
        }
        .workload-card-display p strong {
            font-size: 1em;
        }

        /* Estilo para os botões 'Ver Detalhes' */
        div[data-testid^="stButton"] { /* Alvo: o contêiner gerado pelo Streamlit para o botão */
            margin: -5px 0 0 0; /* Ajusta a margem superior para reduzir o espaçamento com o card. Remove as margens horizontais aqui. */
            width: 100%; /* Faz com que o contêiner do botão ocupe 100% da largura da coluna pai. */
        }
        div[data-testid^="stButton"] button {
            width: calc(100% - 20px); /* A largura do botão é 100% do seu contêiner, menos 20px (10px de padding de cada lado do card visual) */
            margin: 0 10px; /* Aplica uma margem horizontal de 10px ao próprio botão, alinhando-o com o conteúdo interno do card visual. */
            font-size: 0.8em; /* Texto menor para o botão */
            padding: 5px; /* Padding menor */
            background-color: #00CC96; /* Cor de fundo */
            color: white; /* Texto branco */
            border-radius: 5px;
            border: none;
            cursor: pointer;
        }
        div[data-testid^="stButton"] button:hover {
            background-color: #00A37D; /* Cor mais escura no hover */
        }


        /* Cores para status de OS Concluídas (7 dias) */
        .completed-os-red {
            color: #EF553B; /* Red */
            font-weight: 700;
        }
        .completed-os-yellow {
            color: #FFA15A; /* Orange/Yellow */
            font-weight: 700;
        }
        .completed-os-green {
            color: #00CC96; /* Green */
            font-weight: 700;
        }

        /* Estilizando o dataframe (tabela de chamados) - Streamlit Nativo */
        /* Mantido por segurança, mas não deve ser usado com os novos cards */
        .stDataFrame {
            border: 1px solid #2a2e3a;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.5);
            margin-bottom: 20px;
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

        /* --- Estilos para os Cards de OS Abertas e Detalhes --- */
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
            color: #FFA15A; /* Cor padrão para footer, pode ser sobrescrita por cores condicionais */
        }

        /* Cores condicionais para os cards de OS */
        .os-card-success {
            background-color: #00CC9608 !important;
            border-left-color: #00CC96 !important;
            .os-card-footer { color: #00CC96; } /* Footer verde para sucesso */
        }
        .os-card-info {
            background-color: #1E90FF08 !important;
            border-left-color: #1E90FF !important;
            .os-card-footer { color: #1E90FF; } /* Footer azul para info/ativas */
        }
        .os-card-warning {
            background-color: #FFA15A08 !important;
            border-left-color: #FFA15A !important;
            .os-card-footer { color: #FFA15A; } /* Footer laranja para aviso */
        }
        .os-card-danger {
            background-color: #EF553B08 !important;
            border-left-color: #EF553B !important;
            .os-card-footer { color: #EF553B; } /* Footer vermelho para perigo */
        }
        .os-card-default {
            background-color: #90929A08 !important;
            border-left-color: #90929A !important;
            .os-card-footer { color: #90929A; } /* Footer cinza para padrão */
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
            margin-bottom: 10px;
        }
        [data-testid="stInfo"] {
            background-color: #FFA15A20 !important;
            border-left: 8px solid #FFA15A !important;
            color: #FFA15A !important;
            font-weight: 600;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.3);
            margin-bottom: 10px;
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
        placeholder_content = st.empty()
        with placeholder_content.container():
            # --- Título Principal do Painel ---
            st.markdown('<div class="main-panel-title"><h1>Painel de Acompanhamento de OS</h1></div>', unsafe_allow_html=True)

            # --- Informação de Última Atualização ---
            current_time_str_utc = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            current_time_br = datetime.now() - timedelta(hours=3)
            current_time_br_str = current_time_br.strftime("%d/%m/%Y %H:%M:%S")
            st.markdown(f"<p class='last-updated'>Última atualização: {current_time_str_utc} (UTC) / {current_time_br_str} (UTC-3)</p>", unsafe_allow_html=True)
            st.markdown("---") # Separador visual

            # --- Obtenção e Processamento de Dados ---
            with st.spinner("Carregando e processando dados do banco de dados..."):
                # Passando refresh_key para st.cache, o cache_data já gerencia a invalidação pelo ttl
                df_raw = obter_ordens_servico(USERNAME, PASSWORD, HOST, PORT, SERVICE)

            if df_raw.empty:
                st.error("Não foi possível carregar os dados das Ordens de Serviço. Verifique a conexão com o banco de dados e as configurações.")
                time.sleep(30)
                st.experimental_rerun() # Força a reinicialização em caso de erro
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

            # FILTRANDO OS PARA PEGAR APENAS AS "EM ABERTO" (Aguardando Início)
            os_aguardando_inicio = df_processed[
                df_processed["status"] == "Em aberto"
            ].copy()

            os_aguardando_inicio = os_aguardando_inicio.sort_values(by="dt_criacao", ascending=True)

            if not os_aguardando_inicio.empty:
                st.success(f"**{len(os_aguardando_inicio)}** Ordens de Serviço atualmente aguardando início. Atenção às mais antigas!")

                # Gera e renderiza os cards HTML personalizados
                os_cards_html = generate_open_os_cards(os_aguardando_inicio)
                st.markdown(os_cards_html, unsafe_allow_html=True)
            else:
                st.info("Parabéns! Nenhuma Ordem de Serviço aguardando início no momento. Produtividade máxima!")

            st.markdown("---") # Separador visual

            # --- Seção de Carga de Trabalho por Responsável ---
            st.markdown("<h2>Carga de Trabalho de Ordens de Serviço Ativas por Responsável</h2>", unsafe_allow_html=True)

            os_em_andamento_ativas = df_processed[
                (df_processed["status"] == "Em andamento") &
                (df_processed["nm_responsavel"].notna())
            ].copy()

            # NOVO CÁLCULO: OS Finalizadas nos Últimos 7 Dias por Responsável
            data_limite_7_dias = datetime.now() - timedelta(days=7)
            os_finalizadas_ultimos_7_dias = df_processed[
                (df_processed["status"] == "Concluída") &
                (df_processed["nm_responsavel"].notna()) &
                (df_processed["dt_termino"] >= data_limite_7_dias)
            ].copy()

            # Agrupa e conta as OS finalizadas
            contagem_finalizadas = os_finalizadas_ultimos_7_dias["nm_responsavel"].value_counts().reset_index()
            contagem_finalizadas.columns = ["Responsável", "OS Finalizadas (7 dias)"]

            if not os_em_andamento_ativas.empty or not contagem_finalizadas.empty: # Condição para exibir se há OS ativas OU finalizadas
                carga_por_responsavel = os_em_andamento_ativas["nm_responsavel"].value_counts().reset_index()
                carga_por_responsavel.columns = ["Responsável", "OS Ativas"]

                # MERGE com as OS finalizadas
                carga_por_responsavel = pd.merge(
                    carga_por_responsavel,
                    contagem_finalizadas,
                    on="Responsável",
                    how="outer" # Usamos 'outer' para incluir responsáveis que só tenham OS ativas OU só tenham finalizadas
                ).fillna(0) # Preenche NaN com 0 para responsáveis que não tenham a outra categoria

                # Garante que as contagens sejam inteiros
                carga_por_responsavel["OS Ativas"] = carga_por_responsavel["OS Ativas"].astype(int)
                carga_por_responsavel["OS Finalizadas (7 dias)"] = carga_por_responsavel["OS Finalizadas (7 dias)"].astype(int)

                # Opcional: Ordenar para uma melhor visualização, talvez por OS Ativas
                carga_por_responsavel = carga_por_responsavel.sort_values(by="OS Ativas", ascending=False)

                # --- Lógica da Coroa para o Melhor Desempenho ---
                # Encontra o responsável com mais OS finalizadas E menor carga ativa
                best_performer_name = None
                if not carga_por_responsavel.empty:
                    # Ordena primeiro por OS Finalizadas (desc) e depois por OS Ativas (asc)
                    # Isso garante que quem fez mais e tem menos carga venha primeiro
                    sorted_for_crown = carga_por_responsavel.sort_values(
                        by=["OS Finalizadas (7 dias)", "OS Ativas"],
                        ascending=[False, True]
                    )
                    best_performer_name = sorted_for_crown.iloc[0]["Responsável"]

                # Inicializa a variável de estado da sessão para armazenar o responsável selecionado
                if 'selected_responsible' not in st.session_state:
                    st.session_state.selected_responsible = None

                # Criar 9 colunas para os cards de responsáveis
                cols_resp = st.columns(9)

                for idx, row in carga_por_responsavel.iterrows():
                    if idx < 9: # Limita a exibição aos primeiros 9 responsáveis nas 9 colunas
                        with cols_resp[idx]:
                            responsible_name = row['Responsável']
                            os_ativas = row['OS Ativas']
                            os_finalizadas = row['OS Finalizadas (7 dias)']

                            # --- Lógica de Cores para OS Finalizadas ---
                            completed_os_class = "completed-os-red" # Padrão: Vermelho (< 3)
                            if os_finalizadas > 10:
                                completed_os_class = "completed-os-green" # Verde (> 10)
                            elif os_finalizadas > 3: # Amarelo (entre 3 e 10)
                                completed_os_class = "completed-os-yellow"

                            # Adiciona a coroa se for o melhor performer
                            crown_emoji = ""
                            if responsible_name == best_performer_name:
                                crown_emoji = "�� " # Adiciona a coroa

                            # --- RENDERIZA O CARD VISUALMENTE (NÃO CLICÁVEL DIRETAMENTE) ---
                            # Usamos st.markdown para renderizar o HTML estilizado do card.
                            # Este div agora é apenas para exibição.
                            card_html_display = f"""
                            <div class="workload-card-display">
                                <h4>{crown_emoji}{responsible_name}</h4>
                                <p><strong>{os_ativas}</strong> OS Ativas</p>
                                <p><span class="{completed_os_class}"><strong>{os_finalizadas}</strong> OS Concluídas (7 dias)</span></p>
                            </div>
                            """
                            st.markdown(card_html_display, unsafe_allow_html=True)

                            # --- CRIA UM BOTÃO SEPARADO PARA A CLICABILIDADE ---
                            # Este é um st.button padrão, que não aceita HTML no label.
                            # Ele ficará logo abaixo do card visual.
                            if st.button(f"Ver Detalhes", key=f"select_resp_button_{responsible_name}"):
                                st.session_state.selected_responsible = responsible_name
                                st.experimental_rerun() # Força a atualização para mostrar os detalhes
                    else:
                        break # Se tiver mais de 9, paramos de exibir nesta seção
            else:
                st.info("Nenhuma Ordem de Serviço ativa ou concluída recentemente atribuída a um responsável no momento. Todos prontos para mais tarefas!")

            st.markdown("---") # Separador visual

            # --- Seção de Detalhes do Responsável (Exibida ao Clicar) ---
            if st.session_state.selected_responsible:
                st.markdown(f"<h2>Detalhes para {st.session_state.selected_responsible}</h2>", unsafe_allow_html=True)

                selected_resp_df = df_processed[df_processed['nm_responsavel'] == st.session_state.selected_responsible]

                # Detalhes das OS Ativas para o responsável selecionado
                active_os_details = selected_resp_df[selected_resp_df['status'] == 'Em andamento']
                if not active_os_details.empty:
                    st.markdown(f"<h3>OS Ativas de {st.session_state.selected_responsible}: ({len(active_os_details)})</h3>", unsafe_allow_html=True)
                    # Renderiza os cards de OS ativas
                    st.markdown(generate_os_details_cards(active_os_details, card_type="active"), unsafe_allow_html=True)
                else:
                    st.info(f"Nenhuma OS ativa para {st.session_state.selected_responsible}.")

                st.markdown("<br>", unsafe_allow_html=True) # Adiciona um espaço para separar

                # Detalhes das OS Concluídas nos últimos 7 dias para o responsável selecionado
                data_limite_7_dias = datetime.now() - timedelta(days=7)
                completed_os_details = selected_resp_df[
                    (selected_resp_df['status'] == 'Concluída') &
                    (selected_resp_df['dt_termino'].notna()) & # Garante que dt_termino não é NaN
                    (selected_resp_df['dt_termino'] >= data_limite_7_dias)
                ]
                if not completed_os_details.empty:
                    st.markdown(f"<h3>OS Concluídas (Últimos 7 Dias) por {st.session_state.selected_responsible}: ({len(completed_os_details)})</h3>", unsafe_allow_html=True)
                    # Renderiza os cards de OS concluídas
                    st.markdown(generate_os_details_cards(completed_os_details, card_type="completed"), unsafe_allow_html=True)
                else:
                    st.info(f"Nenhuma OS concluída nos últimos 7 dias por {st.session_state.selected_responsible}.")
            else:
                st.info("Clique em um responsável acima para ver seus detalhes de carga e OS concluídas no período!")

        # Pausa o script por 30 segundos antes da próxima atualização
        time.sleep(30)
        # Força a reinicialização do script, o que efetivamente "atualiza" a página
        st.experimental_rerun()

# Ponto de entrada da aplicação Streamlit
if __name__ == "__main__":
    main()
