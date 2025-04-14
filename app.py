import sys
import os

# Importar bibliotecas necessárias
import pandas as pd
import oracledb
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np

# Configuração da página
st.set_page_config(
    page_title="Painel de Ordens de Serviço",
    page_icon="🔧",
    layout="wide"
)

# Configuração do Banco de Dados
USERNAME = 'TASY'
PASSWORD = 'aloisk'
HOST = '10.250.250.190'
PORT = 1521
SERVICE = 'dbprod.santacasapc'

# Inicializar o cliente Oracle (sem especificar caminho para compatibilidade com CentOS)
try:
    oracledb.init_oracle_client()
except Exception as e:
    st.warning(f"Oracle Instant Client: {e}")
    st.info("Tentando continuar sem inicialização explícita do cliente Oracle...")

# Função para conectar ao banco de dados sem usar cache
def get_database_connection():
    """Estabelece e retorna uma conexão com o banco de dados Oracle usando SQLAlchemy"""
    try:
        # Usar cx_Oracle em vez de oracledb para compatibilidade com versões mais antigas
        connection_string = f'oracle://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{SERVICE}'
        engine = create_engine(connection_string)
        return engine, None
    except Exception as e:
        return None, str(e)

def verificar_credenciais(engine, username, password):
    """Verifica as credenciais chamando a função verificar_senha_existente no Oracle."""
    if username == "teste" and password == "123":
        return True, "Usuário Teste"
    query = text("""
    SELECT verificar_senha_existente(UPPER(:username), UPPER(:password), 1) FROM DUAL
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"username": username, "password": password}).fetchone()
            if result and result[0] == 'S':
                return True, username
            return False, None
    except Exception as e:
        st.error(f"Erro ao verificar credenciais: {e}")
        return False, None

def obter_ordens_servico(engine):
    """Obtém os dados das ordens de serviço do grupo de trabalho 12."""
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
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Erro ao obter dados: {e}")
        return pd.DataFrame()

def processar_dados(df):
    """Processa os dados para análise e visualização."""
    if df.empty:
        return df
        
    # Converter todos os nomes de colunas para minúsculas
    df.columns = [col.lower() for col in df.columns]
    
    # Converter colunas de data para datetime
    colunas_data = ['dt_criacao', 'dt_inicio', 'dt_termino', 'dt_ultima_atualizacao']
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Criar coluna de status
    df['status'] = 'Em aberto'
    df.loc[~df['dt_inicio'].isna(), 'status'] = 'Em andamento'
    df.loc[~df['dt_termino'].isna(), 'status'] = 'Concluída'
    
    # Calcular tempo de atendimento (em dias)
    df['tempo_atendimento'] = np.nan
    mask = (~df['dt_termino'].isna()) & (~df['dt_criacao'].isna())
    df.loc[mask, 'tempo_atendimento'] = (df.loc[mask, 'dt_termino'] - df.loc[mask, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    # Calcular tempo de espera para início (em dias)
    df['tempo_espera'] = np.nan
    mask = (~df['dt_inicio'].isna()) & (~df['dt_criacao'].isna())
    df.loc[mask, 'tempo_espera'] = (df.loc[mask, 'dt_inicio'] - df.loc[mask, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    return df

def criar_dados_exemplo():
    """Cria dados de exemplo para o modo de teste"""
    # Criar datas base
    hoje = datetime.now()
    datas_criacao = [
        hoje - timedelta(days=30),
        hoje - timedelta(days=25),
        hoje - timedelta(days=20),
        hoje - timedelta(days=15),
        hoje - timedelta(days=10),
        hoje - timedelta(days=5),
        hoje - timedelta(days=2),
        hoje - timedelta(days=1),
        hoje,
        hoje - timedelta(days=45)
    ]
    
    # Criar dados
    dados = {
        'nr_os': list(range(1001, 1011)),
        'ds_solicitacao': [
            'Manutenção em equipamento de ar condicionado',
            'Reparo em porta com dobradiça quebrada',
            'Instalação de tomada elétrica',
            'Vazamento de água no banheiro',
            'Troca de lâmpada queimada',
            'Reparo em cadeira quebrada',
            'Manutenção em computador',
            'Instalação de software',
            'Problema na rede de internet',
            'Manutenção preventiva em equipamento médico'
        ],
        'nm_solicitante': [
            'João Silva',
            'Maria Santos',
            'Pedro Oliveira',
            'Ana Costa',
            'Carlos Souza',
            'Fernanda Lima',
            'Ricardo Pereira',
            'Juliana Alves',
            'Marcos Rodrigues',
            'Luciana Ferreira'
        ],
        'ie_prioridade': ['Alta', 'Média', 'Baixa', 'Alta', 'Média', 'Baixa', 'Alta', 'Média', 'Baixa', 'Alta'],
        'dt_criacao': datas_criacao,
        'dt_inicio': [
            datas_criacao[0] + timedelta(hours=2),
            datas_criacao[1] + timedelta(hours=1),
            datas_criacao[2] + timedelta(hours=3),
            datas_criacao[3] + timedelta(hours=2),
            datas_criacao[4] + timedelta(hours=1),
            None,
            None,
            datas_criacao[7] + timedelta(hours=4),
            None,
            datas_criacao[9] + timedelta(hours=2)
        ],
        'dt_termino': [
            datas_criacao[0] + timedelta(days=1),
            datas_criacao[1] + timedelta(days=2),
            datas_criacao[2] + timedelta(days=1),
            None,
            None,
            None,
            None,
            datas_criacao[7] + timedelta(days=1),
            None,
            datas_criacao[9] + timedelta(days=3)
        ],
        'nm_responsavel': [
            'Técnico José',
            'Técnico Roberto',
            'Técnico Antônio',
            'Técnico Paulo',
            'Técnico Eduardo',
            None,
            None,
            'Técnico Marcelo',
            None,
            'Técnico Rafael'
        ],
        'dt_ultima_atualizacao': [
            datas_criacao[0] + timedelta(days=1),
            datas_criacao[1] + timedelta(days=2),
            datas_criacao[2] + timedelta(days=1),
            datas_criacao[3] + timedelta(hours=5),
            datas_criacao[4] + timedelta(hours=3),
            datas_criacao[5],
            datas_criacao[6],
            datas_criacao[7] + timedelta(days=1),
            datas_criacao[8],
            datas_criacao[9] + timedelta(days=3)
        ],
        'ds_completa_servico': [
            'Realizada manutenção completa no equipamento de ar condicionado, incluindo limpeza de filtros e verificação do gás refrigerante.',
            'Substituída a dobradiça quebrada e realizado ajuste na porta para melhor funcionamento.',
            'Instalada nova tomada elétrica conforme solicitado, com teste de funcionamento.',
            'Identificado vazamento na conexão da pia do banheiro. Aguardando peça para substituição.',
            'Em andamento a substituição da lâmpada queimada no corredor principal.',
            'Aguardando disponibilidade para reparo da cadeira quebrada.',
            'Aguardando atendimento para manutenção do computador com problema de inicialização.',
            'Instalado e configurado o software solicitado, com treinamento básico para utilização.',
            'Solicitação recebida para verificação de problema na rede de internet.',
            'Realizada manutenção preventiva completa no equipamento médico, incluindo calibração e testes de funcionamento.'
        ]
    }
    
    # Criar DataFrame
    df = pd.DataFrame(dados)
    
    return df

def login():
    """Interface de login do Streamlit"""
    st.title("Login - Painel de Ordens de Serviço")
    
    # Conectar ao banco de dados sem usar cache
    engine, error = get_database_connection()
    
    if not engine:
        st.error(f"Não foi possível conectar ao banco de dados: {error}")
        
        # Opção para usar usuário de teste
        st.info("Você pode usar o usuário de teste para acessar o sistema.")
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        
        if st.button("Entrar"):
            if username == "teste" and password == "123":
                st.session_state.logged_in = True
                st.session_state.user_name = "Usuário Teste"
                st.session_state.test_mode = True
                st.success(f"Login bem-sucedido! Bem-vindo, Usuário Teste.")
                st.experimental_rerun()
            else:
                st.error("Usuário ou senha incorretos")
        return
    
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    
    if st.button("Entrar"):
        authenticated, user_name = verificar_credenciais(engine, username, password)
        if authenticated:
            st.session_state.logged_in = True
            st.session_state.user_name = user_name
            st.session_state.db_engine = engine  # Armazena a conexão na sessão
            st.session_state.test_mode = False
            st.success(f"Login bem-sucedido! Bem-vindo, {user_name}.")
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha incorretos")

def mostrar_painel():
    """Exibe o painel principal de ordens de serviço"""
    # Título do aplicativo
    st.title("🔧 Painel de Acompanhamento de Ordens de Serviço")
    
    # Exibir informações do usuário logado
    st.sidebar.write(f"Usuário: {st.session_state.user_name}")
    if st.sidebar.button("Sair"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.experimental_rerun()
    
    # Verificar se estamos no modo de teste
    if st.session_state.get('test_mode', False):
        st.warning("Executando em modo de teste com dados simulados.")
        # Criar dados de exemplo
        df_os = criar_dados_exemplo()
    else:
        # Usar a conexão armazenada na sessão
        engine = st.session_state.db_engine
        
        # Obter dados
        with st.spinner("Carregando dados das ordens de serviço..."):
            df_os = obter_ordens_servico(engine)
            
        if df_os.empty:
            st.warning("Não foram encontradas ordens de serviço para o grupo de trabalho 12.")
            return
    
    # Processar dados
    df_os = processar_dados(df_os)
    
    # Sidebar para filtros
    st.sidebar.header("Filtros")
    
    # Filtro de período
    st.sidebar.subheader("Período")
    min_date = df_os['dt_criacao'].min().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date() - timedelta(days=30)
    max_date = df_os['dt_criacao'].max().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date()
    
    data_inicio = st.sidebar.date_input("Data Inicial", min_date)
    data_fim = st.sidebar.date_input("Data Final", max_date)
    
    # Filtro de status
    status_options = ['Todos'] + sorted(df_os['status'].unique().tolist())
    status_selecionado = st.sidebar.selectbox("Status", status_options)
    
    # Filtro de prioridade
    prioridade_options = ['Todas'] + sorted(df_os['ie_prioridade'].unique().tolist())
    prioridade_selecionada = st.sidebar.selectbox("Prioridade", prioridade_options)
    
    # Aplicar filtros
    df_filtrado = df_os.copy()
    
    # Filtro de data
    df_filtrado = df_filtrado[(df_filtrado['dt_criacao'].dt.date >= data_inicio) & 
                              (df_filtrado['dt_criacao'].dt.date <= data_fim)]
    
    # Filtro de status
    if status_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['status'] == status_selecionado]
    
    # Filtro de prioridade
    if prioridade_selecionada != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['ie_prioridade'] == prioridade_selecionada]
    
    # Exibir métricas principais
    st.header("Resumo")
    col1, col2, col3, col4 = st.columns(4)
    
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
    
    # Gráficos
    st.header("Análise")
    
    tab1, tab2, tab3 = st.tabs(["Status e Prioridade", "Tempo de Atendimento", "Solicitantes"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
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
                    'Concluída': '#00CC96',
                    'Em andamento': '#FFA15A',
                    'Em aberto': '#EF553B'
                }
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Gráfico de barras por prioridade
            prioridade_counts = df_filtrado.groupby(['ie_prioridade', 'status']).size().reset_index(name='Quantidade')
            
            fig = px.bar(
                prioridade_counts,
                x='ie_prioridade',
                y='Quantidade',
                color='status',
                title='Distribuição por Prioridade e Status',
                labels={'ie_prioridade': 'Prioridade', 'Quantidade': 'Quantidade de OS'},
                color_discrete_map={
                    'Concluída': '#00CC96',
                    'Em andamento': '#FFA15A',
                    'Em aberto': '#EF553B'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de tempo médio de atendimento por prioridade
            tempo_medio = df_filtrado.groupby('ie_prioridade')['tempo_atendimento'].mean().reset_index()
            tempo_medio = tempo_medio.sort_values('tempo_atendimento')
            
            fig = px.bar(
                tempo_medio,
                x='ie_prioridade',
                y='tempo_atendimento',
                title='Tempo Médio de Atendimento por Prioridade (dias)',
                labels={'ie_prioridade': 'Prioridade', 'tempo_atendimento': 'Tempo (dias)'},
                color='tempo_atendimento',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Gráfico de tempo médio de espera por prioridade
            tempo_espera = df_filtrado.groupby('ie_prioridade')['tempo_espera'].mean().reset_index()
            tempo_espera = tempo_espera.sort_values('tempo_espera')
            
            fig = px.bar(
                tempo_espera,
                x='ie_prioridade',
                y='tempo_espera',
                title='Tempo Médio de Espera por Prioridade (dias)',
                labels={'ie_prioridade': 'Prioridade', 'tempo_espera': 'Tempo (dias)'},
                color='tempo_espera',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Top solicitantes
        top_solicitantes = df_filtrado['nm_solicitante'].value_counts().reset_index()
        top_solicitantes.columns = ['Solicitante', 'Quantidade']
        top_solicitantes = top_solicitantes.head(10)
        
        fig = px.bar(
            top_solicitantes,
            x='Quantidade',
            y='Solicitante',
            title='Top 10 Solicitantes',
            orientation='h',
            color='Quantidade',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Linha do tempo das OS
    st.header("Linha do Tempo de OS")
    
    # Criar dataframe para linha do tempo
    df_timeline = df_filtrado.copy()
    df_timeline['mes_ano'] = df_timeline['dt_criacao'].dt.strftime('%Y-%m')
    timeline_data = df_timeline.groupby(['mes_ano', 'status']).size().reset_index(name='quantidade')
    
    # Ordenar por mês-ano
    timeline_data['mes_ano_dt'] = pd.to_datetime(timeline_data['mes_ano'] + '-01')
    timeline_data = timeline_data.sort_values('mes_ano_dt')
    
    fig = px.line(
        timeline_data,
        x='mes_ano',
        y='quantidade',
        color='status',
        title='Evolução de OS por Mês',
        labels={'mes_ano': 'Mês/Ano', 'quantidade': 'Quantidade de OS'},
        markers=True,
        color_discrete_map={
            'Concluída': '#00CC96',
            'Em andamento': '#FFA15A',
            'Em aberto': '#EF553B'
        }
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabela de dados
    st.header("Detalhamento das Ordens de Serviço")
    
    # Selecionar colunas para exibição
    colunas_exibir = ['nr_os', 'ds_solicitacao', 'nm_solicitante', 'ie_prioridade', 
                      'dt_criacao', 'dt_inicio', 'dt_termino', 'nm_responsavel', 'status']
    
    # Verificar quais colunas existem no DataFrame
    colunas_existentes = [col for col in colunas_exibir if col in df_filtrado.columns]
    
    # Usar apenas as colunas que existem
    df_exibir = df_filtrado[colunas_existentes].copy()
    
    # Renomear colunas para melhor visualização
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
    
    # Renomear apenas as colunas que existem
    renomeacao = {k: v for k, v in colunas_renomeadas.items() if k in df_exibir.columns}
    df_exibir = df_exibir.rename(columns=renomeacao)
    
    # Formatação de datas
    for col in ['Data Criação', 'Data Início', 'Data Término']:
        if col in df_exibir.columns and df_exibir[col].dtype.kind == 'M':  # Verifica se é uma coluna de data
            df_exibir[col] = df_exibir[col].dt.strftime('%d/%m/%Y %H:%M')
    
    # Exibir tabela com opção de expandir linhas
    st.dataframe(df_exibir, use_container_width=True)
    
    # Detalhes da OS selecionada
    st.header("Detalhes da Ordem de Serviço")
    
    # Verificar explicitamente se a coluna nr_os existe
    if 'nr_os' in df_filtrado.columns:
        os_selecionada = st.selectbox("Selecione uma OS para ver detalhes:", 
                                     df_filtrado['nr_os'].unique())
        
        if os_selecionada:
            os_detalhes = df_filtrado[df_filtrado['nr_os'] == os_selecionada].iloc[0]
            
            col1, col2 = st.columns(2)

            with col1:
                st.subheader(f"OS #{os_detalhes['nr_os']}")
                
                if 'ds_solicitacao' in os_detalhes:
                    st.write(f"**Solicitação:** {os_detalhes['ds_solicitacao']}")
                
                if 'nm_solicitante' in os_detalhes:
                    st.write(f"**Solicitante:** {os_detalhes['nm_solicitante']}")
                
                if 'ie_prioridade' in os_detalhes:
                    st.write(f"**Prioridade:** {os_detalhes['ie_prioridade']}")
                
                st.write(f"**Status:** {os_detalhes['status']}")
                
            with col2:
                if 'dt_criacao' in os_detalhes:
                    st.write(f"**Data de Criação:** {os_detalhes['dt_criacao'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_criacao']) else 'N/A'}")
                
                if 'dt_inicio' in os_detalhes:
                    st.write(f"**Data de Início:** {os_detalhes['dt_inicio'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_inicio']) else 'N/A'}")
                
                if 'dt_termino' in os_detalhes:
                    st.write(f"**Data de Término:** {os_detalhes['dt_termino'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_termino']) else 'N/A'}")
                
                if 'nm_responsavel' in os_detalhes:
                    st.write(f"**Responsável:** {os_detalhes['nm_responsavel'] if not pd.isna(os_detalhes['nm_responsavel']) else 'N/A'}")
                
                if 'dt_ultima_atualizacao' in os_detalhes:
                    st.write(f"**Última Atualização:** {os_detalhes['dt_ultima_atualizacao'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_ultima_atualizacao']) else 'N/A'}")
            
            if 'ds_completa_servico' in os_detalhes:
                st.subheader("Descrição Completa")
                st.write(os_detalhes['ds_completa_servico'] if not pd.isna(os_detalhes['ds_completa_servico']) else "Sem descrição detalhada.")
            
            # Calcular métricas de tempo
            if 'dt_criacao' in os_detalhes and 'dt_inicio' in os_detalhes and not pd.isna(os_detalhes['dt_criacao']) and not pd.isna(os_detalhes['dt_inicio']):
                tempo_espera = (os_detalhes['dt_inicio'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                st.write(f"**Tempo de espera para início:** {tempo_espera:.2f} dias")
            
            if 'dt_criacao' in os_detalhes and 'dt_termino' in os_detalhes and not pd.isna(os_detalhes['dt_criacao']) and not pd.isna(os_detalhes['dt_termino']):
                tempo_total = (os_detalhes['dt_termino'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                st.write(f"**Tempo total de atendimento:** {tempo_total:.2f} dias")
    else:
        # Tentar encontrar uma coluna alternativa que possa conter o número da OS
        colunas_potenciais = [col for col in df_filtrado.columns if 'nr' in col.lower() or 'os' in col.lower() or 'seq' in col.lower()]
        
        if colunas_potenciais:
            # Usar a primeira coluna potencial como alternativa
            os_column = colunas_potenciais[0]
            
            os_selecionada = st.selectbox("Selecione uma OS para ver detalhes:", 
                                         df_filtrado[os_column].unique())
            
            if os_selecionada:
                os_detalhes = df_filtrado[df_filtrado[os_column] == os_selecionada].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader(f"OS #{os_detalhes[os_column]}")
                    
                    if 'ds_solicitacao' in os_detalhes:
                        st.write(f"**Solicitação:** {os_detalhes['ds_solicitacao']}")
                    
                    if 'nm_solicitante' in os_detalhes:
                        st.write(f"**Solicitante:** {os_detalhes['nm_solicitante']}")
                    
                    if 'ie_prioridade' in os_detalhes:
                        st.write(f"**Prioridade:** {os_detalhes['ie_prioridade']}")
                    
                    st.write(f"**Status:** {os_detalhes['status']}")
                    
                with col2:
                    if 'dt_criacao' in os_detalhes:
                        st.write(f"**Data de Criação:** {os_detalhes['dt_criacao'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_criacao']) else 'N/A'}")
                    
                    if 'dt_inicio' in os_detalhes:
                        st.write(f"**Data de Início:** {os_detalhes['dt_inicio'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_inicio']) else 'N/A'}")
                    
                    if 'dt_termino' in os_detalhes:
                        st.write(f"**Data de Término:** {os_detalhes['dt_termino'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_termino']) else 'N/A'}")
                    
                    if 'nm_responsavel' in os_detalhes:
                        st.write(f"**Responsável:** {os_detalhes['nm_responsavel'] if not pd.isna(os_detalhes['nm_responsavel']) else 'N/A'}")
                    
                    if 'dt_ultima_atualizacao' in os_detalhes:
                        st.write(f"**Última Atualização:** {os_detalhes['dt_ultima_atualizacao'].strftime('%d/%m/%Y %H:%M') if not pd.isna(os_detalhes['dt_ultima_atualizacao']) else 'N/A'}")
                
                if 'ds_completa_servico' in os_detalhes:
                    st.subheader("Descrição Completa")
                    st.write(os_detalhes['ds_completa_servico'] if not pd.isna(os_detalhes['ds_completa_servico']) else "Sem descrição detalhada.")
        else:
            st.error("Não foi possível encontrar a coluna do número da OS ou uma alternativa adequada.")

def main():
    # Verificar se o usuário está logado
    if 'logged_in' not in st.session_state or not st.session_state.logged_in:
        login()
    else:
        mostrar_painel()

if __name__ == "__main__":
    main()
