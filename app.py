import pandas as pd
import oracledb
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import traceback
import sys

# Configuração da página do Streamlit
st.set_page_config(
    page_title="Painel de Ordens de Serviço",
    page_icon="🔧",
    layout="wide"
)

# Configuração do Banco de Dados Oracle
# ATENÇÃO: Substitua os valores abaixo com as suas credenciais reais e informações do banco
USERNAME = 'TASY'
PASSWORD = 'aloisk'
HOST = '10.250.250.190'
PORT = 1521
SERVICE = 'dbprod.santacasapc'

# Inicializa o cliente Oracle Instant Client
# Em ambientes Linux/macOS com o Instant Client configurado corretamente,
# oracledb geralmente encontra as bibliotecas sem a necessidade de um path explícito.
try:
    oracledb.init_oracle_client()
except Exception as e:
    st.sidebar.error(f"Erro na inicialização do Oracle Instant Client: {e}")
    st.sidebar.info("Certifique-se de que o Oracle Instant Client está instalado e configurado corretamente no seu sistema.")

# Função para conectar ao banco de dados Oracle
# Usando st.cache com allow_output_mutation=True para compatibilidade com versões antigas do Streamlit
@st.cache(allow_output_mutation=True, suppress_st_warning=True) # <-- CORREÇÃO AQUI
def conectar_ao_banco():
    """Estabelece uma conexão direta com o banco de dados Oracle usando oracledb."""
    try:
        # Tentativa 1: Usando DSN com formato padrão
        conn = oracledb.connect(user=USERNAME, password=PASSWORD, 
                               dsn=f"{HOST}:{PORT}/{SERVICE}")
        return conn
    except Exception as e:
        try:
            # Tentativa 2: Usando formato de conexão EZ
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={HOST})(PORT={PORT}))(CONNECT_DATA=(SERVICE_NAME={SERVICE})))"
            conn = oracledb.connect(user=USERNAME, password=PASSWORD, dsn=dsn)
            return conn
        except Exception as e2:
            st.error(f"Erro ao conectar ao banco de dados: {e2}")
            st.error("Verifique as credenciais, o endereço do servidor e se o serviço do banco de dados está ativo.")
            return None

# Função para obter dados das ordens de serviço
# Usando st.cache para compatibilidade com versões antigas do Streamlit
@st.cache(allow_output_mutation=True, suppress_st_warning=True) # <-- CORREÇÃO AQUI
def obter_ordens_servico(conn):
    """Obtém os dados das ordens de serviço do grupo de trabalho 12."""
    if conn is None:
        return pd.DataFrame() # Retorna DataFrame vazio se a conexão for nula
    try:
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
        
        # Usar pandas para ler diretamente da conexão
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        st.error("Verifique a query SQL ou se o banco de dados está acessível e a tabela MAN_ORDEM_SERVICO existe.")
        return pd.DataFrame()

# Função para processar e enriquecer os dados
def processar_dados(df):
    """Processa os dados para análise e visualização."""
    if df.empty:
        return df # Retorna o DataFrame vazio se não houver dados para processar

    # Converter todos os nomes de colunas para minúsculas para padronização
    df.columns = [col.lower() for col in df.columns]
    
    # Converter colunas de data para datetime, lidando com erros (coerce)
    colunas_data = ['dt_criacao', 'dt_inicio', 'dt_termino', 'dt_ultima_atualizacao']
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Criar coluna de status
    # Inicializa todas como 'Em aberto'
    df['status'] = 'Em aberto'
    # Se dt_inicio não for nulo, status é 'Em andamento'
    df.loc[df['dt_inicio'].notna(), 'status'] = 'Em andamento'
    # Se dt_termino não for nulo, status é 'Concluída'
    df.loc[df['dt_termino'].notna(), 'status'] = 'Concluída'
    
    # Calcular tempo de atendimento (em dias) para OS Concluídas
    df['tempo_atendimento'] = np.nan # Inicializa com NaN
    mask_atendimento = (df['dt_termino'].notna()) & (df['dt_criacao'].notna())
    df.loc[mask_atendimento, 'tempo_atendimento'] = \
        (df.loc[mask_atendimento, 'dt_termino'] - df.loc[mask_atendimento, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    # Calcular tempo de espera para início (em dias) para OS Iniciadas
    df['tempo_espera'] = np.nan # Inicializa com NaN
    mask_espera = (df['dt_inicio'].notna()) & (df['dt_criacao'].notna())
    df.loc[mask_espera, 'tempo_espera'] = \
        (df.loc[mask_espera, 'dt_inicio'] - df.loc[mask_espera, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    return df

# Função principal do aplicativo Streamlit
def main():
    # Título do aplicativo na página
    st.title("🔧 Painel de Acompanhamento de Ordens de Serviço")
    
    # Conectar ao banco de dados
    with st.spinner("Conectando ao banco de dados..."):
        conn = conectar_ao_banco()
        
    if conn is None:
        st.error("Não foi possível estabelecer conexão com o banco de dados. O painel não poderá exibir dados.")
        return # Interrompe a execução se a conexão falhar
    
    # Obter dados das Ordens de Serviço
    with st.spinner("Carregando dados das ordens de serviço..."):
        df_os = obter_ordens_servico(conn)
        
    if df_os.empty:
        st.warning("Não foram encontradas ordens de serviço para o grupo de trabalho 12 ou houve um erro na consulta.")
        st.info("Verifique se o grupo de trabalho '12' possui dados ou se a query SQL está correta.")
        return # Interrompe a execução se não houver dados

    # Processar dados obtidos
    df_os = processar_dados(df_os)
    
    # Sidebar para filtros do usuário
    st.sidebar.header("Filtros")
    
    # Filtro de período por data de criação
    st.sidebar.subheader("Período de Criação da OS")
    # Define as datas mínima e máxima disponíveis nos dados ou um período padrão
    min_date_available = df_os['dt_criacao'].min().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date() - timedelta(days=90)
    max_date_available = df_os['dt_criacao'].max().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date()
    
    # Garante que as datas de input não excedam as datas disponíveis ou sejam invertidas
    data_inicio_input = st.sidebar.date_input("Data Inicial", min_date_available, 
                                               min_value=min_date_available, max_value=max_date_available)
    data_fim_input = st.sidebar.date_input("Data Final", max_date_available, 
                                            min_value=min_date_available, max_value=max_date_available)
    
    # Ajuste: Garantir que data_fim_input não seja menor que data_inicio_input
    if data_inicio_input > data_fim_input:
        st.sidebar.error("A Data Inicial não pode ser maior que a Data Final.")
        # Por simplicidade, vamos apenas avisar e deixar que o Streamlit lide com a interface.
        # O DataFrame filtrado ficará vazio até que o usuário corrija.
        df_filtrado = pd.DataFrame()
    else:
        # Aplica o filtro de data antes dos outros filtros para performance
        df_filtrado = df_os[(df_os['dt_criacao'].dt.date >= data_inicio_input) & 
                                (df_os['dt_criacao'].dt.date <= data_fim_input)].copy()
    
    if df_filtrado.empty:
        st.warning("Não há dados para o período de criação selecionado. Ajuste o filtro de datas.")
        return # Interrompe a execução se não houver dados no período
        
    # Filtro de status
    status_options = ['Todos'] + sorted(df_filtrado['status'].unique().tolist())
    status_selecionado = st.sidebar.selectbox("Status", status_options)
    
    # Filtro de prioridade
    prioridade_options = ['Todas'] + sorted(df_filtrado['ie_prioridade'].unique().tolist())
    prioridade_selecionada = st.sidebar.selectbox("Prioridade", prioridade_options)
    
    # Aplica os filtros de status e prioridade
    if status_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['status'] == status_selecionado]
    
    if prioridade_selecionada != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['ie_prioridade'] == prioridade_selecionada]
    
    if df_filtrado.empty:
        st.warning("Nenhuma Ordem de Serviço encontrada com os filtros aplicados. Tente ajustar os filtros.")
        return # Interrompe a execução se não houver dados após os filtros
    
    # Exibir métricas principais (cards de resumo)
    st.header("Resumo Geral das Ordens de Serviço Filtradas")
    col1, col2, col3, col4 = st.columns(4) # Cria 4 colunas para os cards
    
    with col1:
        st.metric("Total de OS", len(df_filtrado))
    
    with col2:
        concluidas = len(df_filtrado[df_filtrado['status'] == 'Concluída'])
        st.metric("Concluídas", concluidas)
    
    with col3:
        em_andamento = len(df_filtrado[df_filtrado['status'] == 'Em andamento'])
        st.metric("Em andamento", em_andamento)
    
    with col4:
        em_aberto = len(df_filtrado[df_filtrado['status'] == 'Em aberto'])
        st.metric("Em aberto", em_aberto)
    
    st.markdown("---") # Separador visual

    # Seção de Análise - Navegação por abas (usando st.radio para compatibilidade)
    st.header("Análise Detalhada das Ordens de Serviço")
    tab_selecionada = st.radio(
        "Selecione uma visualização:",
        ["Status e Prioridade", "Tempo de Atendimento", "Solicitantes", "OS sem Responsável e Carga de Trabalho"],
        horizontal=True # Deixa as opções do radio buttons na horizontal
    )
    
    st.subheader(f"Visualização: {tab_selecionada}")
    
    if tab_selecionada == "Status e Prioridade":
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("#### Distribuição de OS por Status")
            # Gráfico de pizza de status
            status_counts = df_filtrado['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Quantidade']
            
            fig = px.pie(
                status_counts, 
                values='Quantidade', 
                names='Status',
                title='Distribuição por Status',
                color='Status',
                color_discrete_map={
                    'Concluída': '#00CC96',     # Verde
                    'Em andamento': '#FFA15A',  # Laranja
                    'Em aberto': '#EF553B'      # Vermelho
                }
            )
            fig.update_traces(textposition='inside', textinfo='percent+label', 
                              marker=dict(line=dict(color='#000000', width=1))) # Borda para fatias
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.write("#### Quantidade de OS por Prioridade e Status")
            # Gráfico de barras por prioridade e status
            prioridade_counts = df_filtrado.groupby(['ie_prioridade', 'status']).size().reset_index(name='Quantidade')
            
            fig = px.bar(
                prioridade_counts,
                x='ie_prioridade',
                y='Quantidade',
                color='status',
                barmode='group', # Barras agrupadas por prioridade
                title='Distribuição por Prioridade e Status',
                labels={'ie_prioridade': 'Prioridade', 'Quantidade': 'Quantidade de OS'},
                color_discrete_map={
                    'Concluída': '#00CC96',
                    'Em andamento': '#FFA15A',
                    'Em aberto': '#EF553B'
                }
            )
            fig.update_layout(xaxis_title="Prioridade", yaxis_title="Quantidade de OS")
            st.plotly_chart(fig, use_container_width=True)
    
    elif tab_selecionada == "Tempo de Atendimento":
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("#### Tempo Médio de Atendimento por Prioridade")
            # Gráfico de tempo médio de atendimento por prioridade
            # Filtra apenas OS Concluídas para calcular tempo de atendimento
            df_concluidas = df_filtrado[df_filtrado['status'] == 'Concluída']
            tempo_medio = df_concluidas.groupby('ie_prioridade')['tempo_atendimento'].mean().reset_index()
            tempo_medio = tempo_medio.sort_values('tempo_atendimento', ascending=False) # Da maior média para a menor
            
            if not tempo_medio.empty:
                fig = px.bar(
                    tempo_medio,
                    x='ie_prioridade',
                    y='tempo_atendimento',
                    title='Tempo Médio de Atendimento por Prioridade (dias)',
                    labels={'ie_prioridade': 'Prioridade', 'tempo_atendimento': 'Tempo (dias)'},
                    color='tempo_atendimento',
                    color_continuous_scale=px.colors.sequential.Viridis # Escala de cores contínua
                )
                fig.update_layout(xaxis_title="Prioridade", yaxis_title="Tempo Médio (dias)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Não há Ordens de Serviço Concluídas no período selecionado para calcular o tempo médio de atendimento.")
        
        with col2:
            st.write("#### Tempo Médio de Espera para Início por Prioridade")
            # Gráfico de tempo médio de espera por prioridade
            # Filtra apenas OS que já iniciaram para calcular tempo de espera
            df_iniciadas = df_filtrado[df_filtrado['status'].isin(['Em andamento', 'Concluída'])]
            tempo_espera = df_iniciadas.groupby('ie_prioridade')['tempo_espera'].mean().reset_index()
            tempo_espera = tempo_espera.sort_values('tempo_espera', ascending=False) # Da maior média para a menor
            
            if not tempo_espera.empty:
                fig = px.bar(
                    tempo_espera,
                    x='ie_prioridade',
                    y='tempo_espera',
                    title='Tempo Médio de Espera por Prioridade (dias)',
                    labels={'ie_prioridade': 'Prioridade', 'tempo_espera': 'Tempo (dias)'},
                    color='tempo_espera',
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig.update_layout(xaxis_title="Prioridade", yaxis_title="Tempo Médio (dias)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Não há Ordens de Serviço que iniciaram no período selecionado para calcular o tempo médio de espera.")
    
    elif tab_selecionada == "Solicitantes":
        st.write("#### Top 10 Solicitantes com Mais Ordens de Serviço")
        # Top 10 solicitantes
        top_solicitantes = df_filtrado['nm_solicitante'].value_counts().reset_index()
        top_solicitantes.columns = ['Solicitante', 'Quantidade']
        top_solicitantes = top_solicitantes.head(10) # Limita aos 10 primeiros
        
        if not top_solicitantes.empty:
            fig = px.bar(
                top_solicitantes,
                x='Quantidade',
                y='Solicitante',
                title='Top 10 Solicitantes',
                orientation='h', # Barras horizontais
                color='Quantidade',
                color_continuous_scale=px.colors.sequential.Viridis
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'}) # Ordena os solicitantes do menor para o maior na barra
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Não há solicitantes com Ordens de Serviço no período selecionado.")

    # --- NOVA ABA: OS sem Responsável e Carga de Trabalho ---
    elif tab_selecionada == "OS sem Responsável e Carga de Trabalho":
        st.subheader("Ordens de Serviço em Aberto sem Responsável Designado")
        st.write("Esta seção lista as Ordens de Serviço que estão atualmente 'Em aberto' e para as quais nenhum responsável foi atribuído. Elas são ordenadas da mais antiga para a mais recente, ajudando a identificar itens que podem estar parados.")
        
        # 1. Lista de OS em Aberto sem Responsável (Ordenada da mais antiga para a mais nova)
        os_sem_responsavel = df_filtrado[
            (df_filtrado['status'] == 'Em aberto') & 
            (df_filtrado['nm_responsavel'].isna()) # Verifica se o responsável é NaN (nulo)
        ].copy() 

        # Ordena pela data de criação, da mais antiga para a mais nova
        os_sem_responsavel = os_sem_responsavel.sort_values(by='dt_criacao', ascending=True)

        if not os_sem_responsavel.empty:
            st.success(f"Foram encontradas **{len(os_sem_responsavel)}** Ordens de Serviço em aberto sem responsável designada.")
            # Seleciona e renomeia colunas para melhor visualização na tabela
            st.dataframe(os_sem_responsavel[[
                'nr_os', 'ds_solicitacao', 'dt_criacao', 'ie_prioridade', 'status'
            ]].rename(columns={
                'nr_os': 'Nº OS', 
                'ds_solicitacao': 'Solicitação', 
                'dt_criacao': 'Data Criação', 
                'ie_prioridade': 'Prioridade', 
                'status': 'Status'
            }), use_container_width=True)
        else:
            st.info("🎉 Nenhuma Ordem de Serviço em aberto sem responsável designada no período selecionado! Isso é um ótimo sinal de organização!")
            
        st.markdown("---") # Separador visual para a próxima seção
        
        st.subheader("Carga de Trabalho de Ordens de Serviço em Aberto por Responsável")
        st.write("Aqui você pode visualizar a distribuição das Ordens de Serviço que estão 'Em aberto' e já atribuídas a um responsável. Cada card mostra a contagem de OS em aberto para cada técnico, auxiliando na gestão da carga de trabalho.")

        # 2. Cartões para cada Usuário Técnico com a Quantidade de Chamados em Aberto
        # Filtra as OS que estão 'Em aberto' e que possuem um responsável (não-nulo)
        os_com_responsavel_em_aberto = df_filtrado[
            (df_filtrado['status'] == 'Em aberto') & 
            (df_filtrado['nm_responsavel'].notna()) # Verifica se o responsável NÃO é NaN
        ].copy()
        
        if not os_com_responsavel_em_aberto.empty:
            # Conta a quantidade de OS em aberto por responsável
            open_os_per_responsible = os_com_responsavel_em_aberto['nm_responsavel'].value_counts().reset_index()
            open_os_per_responsible.columns = ['Responsável', 'Quantidade']
            
            # Define o número de colunas para os cards (máximo de 3 para melhor visualização)
            num_responsibles = len(open_os_per_responsible)
            num_cols = min(3, num_responsibles if num_responsibles > 0 else 1)
            
            # Cria as colunas no Streamlit
            cols = st.columns(num_cols) 
            col_idx = 0
            
            # Itera sobre cada responsável para criar um card
            for index, row in open_os_per_responsible.iterrows():
                with cols[col_idx % num_cols]: # Distribui os cards entre as colunas
                    st.info(f"**{row['Responsável']}**\n\nOS em Aberto: **{int(row['Quantidade'])}**")
                col_idx += 1
        else:
            st.info("Nenhuma Ordem de Serviço em aberto designada a um responsável no período selecionado.")
    # --- FIM DA NOVA ABA ---
    
    st.markdown("---") # Separador visual

    # Linha do tempo das OS criadas
    st.header("Evolução Mensal das Ordens de Serviço por Status")
    st.write("Este gráfico mostra como o número de Ordens de Serviço em diferentes status (Concluídas, Em Andamento, Em Aberto) evoluiu ao longo do tempo, com base na data de criação.")
    
    # Criar dataframe para linha do tempo
    df_timeline = df_filtrado.copy()
    # Cria uma coluna 'mes_ano' no formato 'AAAA-MM' para agrupamento e ordenação
    df_timeline['mes_ano'] = df_timeline['dt_criacao'].dt.strftime('%Y-%m')
    timeline_data = df_timeline.groupby(['mes_ano', 'status']).size().reset_index(name='quantidade')
    
    # Ordenar por mês-ano para garantir a sequência correta no gráfico
    timeline_data['mes_ano_dt'] = pd.to_datetime(timeline_data['mes_ano'] + '-01')
    timeline_data = timeline_data.sort_values('mes_ano_dt')
    
    if not timeline_data.empty:
        fig = px.line(
            timeline_data,
            x='mes_ano',
            y='quantidade',
            color='status',
            title='Evolução de OS por Mês',
            labels={'mes_ano': 'Mês/Ano', 'quantidade': 'Quantidade de OS'},
            markers=True, # Adiciona marcadores nos pontos de dados
            color_discrete_map={
                'Concluída': '#00CC96',
                'Em andamento': '#FFA15A',
                'Em aberto': '#EF553B'
            }
        )
        fig.update_layout(xaxis_title="Mês/Ano", yaxis_title="Quantidade de OS")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Não há dados de linha do tempo para exibir com os filtros atuais.")
    
    st.markdown("---") # Separador visual

    # Tabela de detalhamento das Ordens de Serviço
    st.header("Detalhamento das Ordens de Serviço Filtradas")
    st.write("Visualize todas as Ordens de Serviço que correspondem aos filtros selecionados, com informações detalhadas para cada uma.")
    
    # Selecionar colunas para exibição na tabela principal
    colunas_exibir = ['nr_os', 'ds_solicitacao', 'nm_solicitante', 'ie_prioridade', 
                      'dt_criacao', 'dt_inicio', 'dt_termino', 'nm_responsavel', 'status']
    
    # Verifica quais colunas da lista existem no DataFrame filtrado
    colunas_existentes = [col for col in colunas_exibir if col in df_filtrado.columns]
    
    # Cria uma cópia do DataFrame com apenas as colunas existentes para exibição
    df_exibir = df_filtrado[colunas_existentes].copy()
    
    # Dicionário para renomear colunas para apresentação amigável
    colunas_renomeadas = {
        'nr_os': 'Nº OS',
        'ds_solicitacao': 'Solicitação',
        'nm_solicitante': 'Solicitante',
        'ie_prioridade': 'Prioridade',
        'dt_criacao': 'Data Criação',
        'dt_inicio': 'Data Início',
        'dt_termino': 'Data Término',
        'nm_responsavel': 'Responsável',
        'status': 'Status'
    }
    
    # Aplica o renomeamento apenas para as colunas que existem no df_exibir
    renomeacao = {k: v for k, v in colunas_renomeadas.items() if k in df_exibir.columns}
    df_exibir = df_exibir.rename(columns=renomeacao)
    
    # Formatação de colunas de data para exibição mais legível
    for col in ['Data Criação', 'Data Início', 'Data Término']:
        if col in df_exibir.columns and df_exibir[col].dtype.kind == 'M':  # Verifica se é uma coluna datetime
            df_exibir[col] = df_exibir[col].dt.strftime('%d/%m/%Y %H:%M').fillna('N/A')
    
    # Exibir tabela interativa
    st.dataframe(df_exibir, use_container_width=True)
    
    st.markdown("---") # Separador visual

    # Detalhes de uma Ordem de Serviço selecionada
    st.header("Detalhes da Ordem de Serviço Selecionada")
    st.write("Selecione uma Ordem de Serviço para ver todos os detalhes, incluindo descrições completas e tempos calculados.")
    
    # Verifica se a coluna 'nr_os' existe no DataFrame filtrado para a seleção
    if 'nr_os' in df_filtrado.columns and not df_filtrado['nr_os'].empty:
        # Cria um selectbox para o usuário escolher uma OS
        os_selecionada_nr = st.selectbox("Selecione o Número da OS para ver detalhes:", 
                                         sorted(df_filtrado['nr_os'].unique().tolist()))
        
        if os_selecionada_nr:
            # Filtra o DataFrame para obter os detalhes da OS selecionada
            os_detalhes = df_filtrado[df_filtrado['nr_os'] == os_selecionada_nr].iloc[0] # Pega a primeira linha correspondente
            
            col_left, col_right = st.columns(2) # Duas colunas para organizar os detalhes
            
            with col_left:
                st.subheader(f"OS #{os_detalhes['nr_os']}")
                
                # Exibe detalhes básicos, verificando a existência da coluna e tratando valores nulos
                if 'ds_solicitacao' in os_detalhes and pd.notna(os_detalhes['ds_solicitacao']):
                    st.write(f"**Solicitação:** {os_detalhes['ds_solicitacao']}")
                else: st.write("**Solicitação:** N/A")
                
                if 'nm_solicitante' in os_detalhes and pd.notna(os_detalhes['nm_solicitante']):
                    st.write(f"**Solicitante:** {os_detalhes['nm_solicitante']}")
                else: st.write("**Solicitante:** N/A")
                
                if 'ie_prioridade' in os_detalhes and pd.notna(os_detalhes['ie_prioridade']):
                    st.write(f"**Prioridade:** {os_detalhes['ie_prioridade']}")
                else: st.write("**Prioridade:** N/A")
                
                if 'status' in os_detalhes and pd.notna(os_detalhes['status']):
                    st.write(f"**Status:** {os_detalhes['status']}")
                else: st.write("**Status:** N/A")
                
            with col_right:
                # Exibe datas e responsável, formatando e tratando nulos
                st.write(f"**Data de Criação:** {os_detalhes['dt_criacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_criacao']) else 'N/A'}")
                st.write(f"**Data de Início:** {os_detalhes['dt_inicio'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_inicio']) else 'N/A'}")
                st.write(f"**Data de Término:** {os_detalhes['dt_termino'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_termino']) else 'N/A'}")
                
                if 'nm_responsavel' in os_detalhes and pd.notna(os_detalhes['nm_responsavel']):
                    st.write(f"**Responsável:** {os_detalhes['nm_responsavel']}")
                else: st.write("**Responsável:** N/A")
                
                st.write(f"**Última Atualização:** {os_detalhes['dt_ultima_atualizacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_ultima_atualizacao']) else 'N/A'}")
            
            # Descrição completa do serviço, se disponível
            if 'ds_completa_servico' in os_detalhes and pd.notna(os_detalhes['ds_completa_servico']):
                st.subheader("Descrição Completa da Ordem de Serviço")
                st.write(os_detalhes['ds_completa_servico'])
            else:
                st.subheader("Descrição Completa da Ordem de Serviço")
                st.info("Sem descrição detalhada disponível para esta OS.")
            
            # Calcular e exibir métricas de tempo para a OS específica
            st.subheader("Métricas de Tempo da OS")
            
            # Tempo de espera para início
            if pd.notna(os_detalhes['dt_criacao']) and pd.notna(os_detalhes['dt_inicio']):
                tempo_espera = (os_detalhes['dt_inicio'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                st.write(f"**Tempo de espera para início:** {tempo_espera:.2f} dias")
            else:
                st.write("**Tempo de espera para início:** N/A (OS não iniciada ou sem data de criação/início)")
            
            # Tempo total de atendimento
            if pd.notna(os_detalhes['dt_criacao']) and pd.notna(os_detalhes['dt_termino']):
                tempo_total = (os_detalhes['dt_termino'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                st.write(f"**Tempo total de atendimento:** {tempo_total:.2f} dias")
            else:
                st.write("**Tempo total de atendimento:** N/A (OS não concluída ou sem data de criação/término)")
    else:
        st.info("Nenhuma Ordem de Serviço disponível para seleção ou a coluna 'nr_os' não foi encontrada.")

# Ponto de entrada da aplicação Streamlit
if __name__ == "__main__":
    main()
