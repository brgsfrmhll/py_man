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

# Inicializa o cliente Oracle Instant Client (sem especificar caminho para Linux)
try:
    oracledb.init_oracle_client()  # No Linux, geralmente não precisa do caminho se instalado corretamente
except Exception as e:
    st.sidebar.error(f"Erro na inicialização do Oracle Instant Client: {e}")

# Usando conexão direta com oracledb em vez de SQLAlchemy
@st.cache(allow_output_mutation=True, suppress_st_warning=True)
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
            # Mostrar mensagem de erro
            st.error(f"Erro ao conectar ao banco de dados: {e2}")
            return None

def obter_ordens_servico(conn):
    """Obtém os dados das ordens de serviço do grupo de trabalho 12."""
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
        # Retornar DataFrame vazio em caso de erro
        return pd.DataFrame()

def processar_dados(df):
    """Processa os dados para análise e visualização."""
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

def main():
    # Título do aplicativo
    st.title("🔧 Painel de Acompanhamento de Ordens de Serviço")
    
    # Conectar ao banco de dados
    with st.spinner("Conectando ao banco de dados..."):
        conn = conectar_ao_banco()
        
    if conn is None:
        st.error("Não foi possível conectar ao banco de dados. Verifique as credenciais.")
        return
    
    # Obter dados
    with st.spinner("Carregando dados das ordens de serviço..."):
        df_os = obter_ordens_servico(conn)
        
    if df_os.empty:
        st.warning("Não foram encontradas ordens de serviço para o grupo de trabalho 12 ou houve um erro na consulta.")
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
    
    # Substituindo st.tabs() por st.radio() para compatibilidade com versões mais antigas
    tab_selecionada = st.radio(
        "Selecione uma visualização:",
        ["Status e Prioridade", "Tempo de Atendimento", "Solicitantes"]
    )
    
    st.subheader(tab_selecionada)
    
    if tab_selecionada == "Status e Prioridade":
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
            st.plotly_chart(fig)
        
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
            st.plotly_chart(fig)
    
    elif tab_selecionada == "Tempo de Atendimento":
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
            st.plotly_chart(fig)
        
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
            st.plotly_chart(fig)
    
    elif tab_selecionada == "Solicitantes":
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
        st.plotly_chart(fig)
    
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
    st.plotly_chart(fig)
    
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
    
    # Exibir tabela sem o parâmetro use_container_width
    st.dataframe(df_exibir)
    
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

if __name__ == "__main__":
    main()
