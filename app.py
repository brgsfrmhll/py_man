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
import time # Importar a biblioteca time para a auto-atualização

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
try:
    oracledb.init_oracle_client()
except Exception as e:
    st.sidebar.error(f"Erro na inicialização do Oracle Instant Client: {e}")
    st.sidebar.info("Certifique-se de que o Oracle Instant Client está instalado e configurado corretamente no seu sistema.")

# Função para conectar ao banco de dados Oracle
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
            st.error(f"Erro ao conectar ao banco de dados: {e2}")
            st.error("Verifique as credenciais, o endereço do servidor e se o serviço do banco de dados está ativo.")
            return None

# Função para obter dados das ordens de serviço
# Adicionamos 'refresh_key' para forçar a atualização a cada 30 segundos
@st.cache(allow_output_mutation=True, suppress_st_warning=True, hash_funcs={oracledb.Connection: lambda _: None})
def obter_ordens_servico(conn, refresh_key): # <-- Adicionado refresh_key
    """Obtém os dados das ordens de serviço do grupo de trabalho 12."""
    if conn is None:
        return pd.DataFrame() 
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
        return df

    df.columns = [col.lower() for col in df.columns]
    
    colunas_data = ['dt_criacao', 'dt_inicio', 'dt_termino', 'dt_ultima_atualizacao']
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Criar coluna de status
    # Baseado na sua nova lógica:
    # 'Em aberto' (Aguarda Início) = dt_inicio IS NULL AND dt_fim IS NULL
    # 'Em andamento' (Ativa) = dt_inicio IS NOT NULL AND dt_fim IS NULL
    # 'Concluída' = dt_fim IS NOT NULL
    df['status'] = 'Concluída' # Default para ser sobrescrito
    df.loc[df['dt_inicio'].isna() & df['dt_termino'].isna(), 'status'] = 'Em aberto'
    df.loc[df['dt_inicio'].notna() & df['dt_termino'].isna(), 'status'] = 'Em andamento'
    
    df['tempo_atendimento'] = np.nan
    mask_atendimento = (df['dt_termino'].notna()) & (df['dt_criacao'].notna())
    df.loc[mask_atendimento, 'tempo_atendimento'] = \
        (df.loc[mask_atendimento, 'dt_termino'] - df.loc[mask_atendimento, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    df['tempo_espera'] = np.nan
    mask_espera = (df['dt_inicio'].notna()) & (df['dt_criacao'].notna())
    df.loc[mask_espera, 'tempo_espera'] = \
        (df.loc[mask_espera, 'dt_inicio'] - df.loc[mask_espera, 'dt_criacao']).dt.total_seconds() / (24*60*60)
    
    return df

# Função para exibir o Painel de Acompanhamento (aba dedicada)
def exibir_painel_acompanhamento(df_filtrado):
    st.subheader("Ordens de Serviço Abertas e Aguardando Início")
    st.write(
        "Esta seção lista as Ordens de Serviço que ainda não foram iniciadas (`dt_inicio` é nulo) e não foram concluídas (`dt_termino` é nulo). "
        "Elas estão ordenadas da mais antiga para a mais recente, ajudando a identificar itens que podem estar parados, independentemente de terem um responsável atribuído."
    )
    
    # Filtra as OS que estão 'Em aberto' (Aguardando Início)
    os_aguardando_inicio = df_filtrado[
        df_filtrado["status"] == "Em aberto" 
    ].copy() 

    # Ordena pela data de criação, da mais antiga para a mais nova
    os_aguardando_inicio = os_aguardando_inicio.sort_values(by="dt_criacao", ascending=True)

    if not os_aguardando_inicio.empty:
        # Exibe uma mensagem de sucesso com a contagem
        st.success(f"Foram encontradas **{len(os_aguardando_inicio)}** Ordens de Serviço abertas e aguardando início.")
        # Exibe o dataframe
        st.dataframe(os_aguardando_inicio[[
            'nr_os', 'ds_solicitacao', 'nm_solicitante', 'ie_prioridade', 'dt_criacao', 'nm_responsavel'
        ]].rename(columns={
            'nr_os': 'Nº OS', 
            'ds_solicitacao': 'Solicitação', 
            'nm_solicitante': 'Solicitante',
            'ie_prioridade': 'Prioridade', 
            'dt_criacao': 'Data Criação',
            'nm_responsavel': 'Responsável Atual' 
        })) # use_container_width removido para compatibilidade
    else:
        st.info("🎉 Nenhuma Ordem de Serviço aberta aguardando início no período selecionado! Bom trabalho!")
        
    st.markdown("---") # Separador visual
    
    st.subheader("Carga de Trabalho de Ordens de Serviço Ativas por Responsável")
    st.write("Aqui você pode visualizar a quantidade de Ordens de Serviço que estão **ativas (já iniciadas e ainda não concluídas)** para cada técnico.")

    # Filtra as OS que estão 'Em andamento' (Ativas)
    # E que possuem um responsável (nm_responsavel IS NOT NULL)
    os_em_andamento_ativas = df_filtrado[
        (df_filtrado["status"] == "Em andamento") & 
        (df_filtrado["nm_responsavel"].notna()) 
    ].copy()
    
    if not os_em_andamento_ativas.empty:
        # Conta a quantidade de OS ativas por responsável
        carga_por_responsavel = os_em_andamento_ativas["nm_responsavel"].value_counts().reset_index()
        carga_por_responsavel.columns = ["Responsável", "OS Ativas"]
        
        # Define o número de colunas para os cards (máximo de 3 para melhor visualização)
        num_responsaveis = len(carga_por_responsavel)
        num_colunas = min(3, num_responsaveis if num_responsaveis > 0 else 1) 
        cols = st.columns(num_colunas) # Usa 'cols' como variável para as colunas

        for idx, row in carga_por_responsavel.iterrows():
            with cols[idx % num_colunas]: # Distribui os cartões entre as colunas
                # Usando st.info para dar cor azul ao card, conforme solicitado
                st.info( 
                    f"**{row['Responsável']}**\n\n"
                    f"OS Ativas: **{int(row['OS Ativas'])}**"
                )
    else:
        st.info("Nenhuma Ordem de Serviço ativa atribuída a um responsável no período selecionado.")


# Função principal do aplicativo Streamlit
def main():
    # Inicializa o estado da sessão para a visualização selecionada
    if 'selected_view' not in st.session_state:
        st.session_state.selected_view = "Painel de Acompanhamento" # Define o painel de acompanhamento como padrão
    
    # Loop infinito para auto-atualização do dashboard
    while True: 
        # Usamos st.empty() para "limpar" o conteúdo anterior e redesenhá-lo completamente
        placeholder_content = st.empty()
        with placeholder_content.container():

            st.title("🔧 Painel de Acompanhamento de Ordens de Serviço")
            
            # --- Sidebar para Navegação ---
            st.sidebar.header("Navegação do Painel")
            # Botões na sidebar para alternar entre as visualizações
            if st.sidebar.button("Status e Prioridade", key="btn_status_prioridade"):
                st.session_state.selected_view = "Status e Prioridade"
            if st.sidebar.button("Tempo de Atendimento", key="btn_tempo_atendimento"):
                st.session_state.selected_view = "Tempo de Atendimento"
            if st.sidebar.button("Solicitantes", key="btn_solicitantes"):
                st.session_state.selected_view = "Solicitantes"
            if st.sidebar.button("Painel de Acompanhamento", key="btn_painel_acompanhamento"):
                st.session_state.selected_view = "Painel de Acompanhamento"
            
            st.sidebar.markdown("---") # Separador visual na sidebar
            # --- Fim da Navegação da Sidebar ---

            # Conectar ao banco de dados
            with st.spinner("Conectando ao banco de dados..."):
                conn = conectar_ao_banco()
                
            if conn is None:
                st.error("Não foi possível estabelecer conexão com o banco de dados. O painel não poderá exibir dados.")
                time.sleep(30) # Espera 30 segundos antes de tentar novamente
                continue # Volta para o início do loop (tenta reconectar)

            # Obter dados das Ordens de Serviço
            # A chave de atualização (time.time() // 30) força o Streamlit a re-executar
            # a função `obter_ordens_servico` a cada 30 segundos, ignorando o cache
            with st.spinner("Carregando dados das ordens de serviço..."):
                df_os = obter_ordens_servico(conn, time.time() // 30) 
                
            if df_os.empty:
                st.warning("Não foram encontradas ordens de serviço para o grupo de trabalho 12 ou houve um erro na consulta.")
                st.info("Verifique se o grupo de trabalho '12' possui dados ou se a query SQL está correta.")
                time.sleep(30) # Espera 30 segundos antes de tentar novamente
                continue # Volta para o início do loop

            # Processar dados obtidos
            df_os = processar_dados(df_os)
            
            # Sidebar para filtros de dados
            st.sidebar.header("Filtros de Dados")
            
            # Filtro de período por data de criação
            st.sidebar.subheader("Período de Criação da OS")
            min_date_available = df_os['dt_criacao'].min().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date() - timedelta(days=90)
            max_date_available = df_os['dt_criacao'].max().date() if not df_os['dt_criacao'].isna().all() else datetime.now().date()
            
            data_inicio_input = st.sidebar.date_input("Data Inicial", min_date_available, 
                                                       min_value=min_date_available, max_value=max_date_available)
            data_fim_input = st.sidebar.date_input("Data Final", max_date_available, 
                                                    min_value=min_date_available, max_value=max_date_available)
            
            if data_inicio_input > data_fim_input:
                st.sidebar.error("A Data Inicial não pode ser maior que a Data Final.")
                df_filtrado = pd.DataFrame() # Esvazia o DataFrame para indicar erro
            else:
                df_filtrado = df_os[(df_os['dt_criacao'].dt.date >= data_inicio_input) & 
                                        (df_os['dt_criacao'].dt.date <= data_fim_input)].copy()
            
            if df_filtrado.empty:
                st.warning("Não há dados para o período de criação selecionado. Ajuste o filtro de datas.")
                time.sleep(30)
                continue
                
            st.sidebar.markdown("---") # Separador visual

            # Filtros adicionais para status e prioridade
            st.sidebar.subheader("Filtros Adicionais")
            status_options = ['Todos'] + sorted(df_filtrado['status'].unique().tolist())
            status_selecionado = st.sidebar.selectbox("Status", status_options)
            
            prioridade_options = ['Todas'] + sorted(df_filtrado['ie_prioridade'].unique().tolist())
            prioridade_selecionada = st.sidebar.selectbox("Prioridade", prioridade_options)
            
            if status_selecionado != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['status'] == status_selecionado]
            
            if prioridade_selecionada != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['ie_prioridade'] == prioridade_selecionada]
            
            if df_filtrado.empty:
                st.warning("Nenhuma Ordem de Serviço encontrada com os filtros aplicados. Tente ajustar os filtros.")
                time.sleep(30)
                continue
            
            # --- Área de Conteúdo Principal ---
            st.header("Resumo Geral das Ordens de Serviço Filtradas")
            col1, col2, col3, col4 = st.columns(4) # Cria 4 colunas para os cards de resumo
            
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
            
            st.markdown("---")

            # Exibe o conteúdo da visualização selecionada
            st.header(f"Visualização: {st.session_state.selected_view}")

            if st.session_state.selected_view == "Status e Prioridade":
                col1, col2 = st.columns(2)
                with col1:
                    st.write("#### Distribuição de OS por Status")
                    status_counts = df_filtrado['status'].value_counts().reset_index()
                    status_counts.columns = ['Status', 'Quantidade']
                    fig = px.pie(status_counts, values='Quantidade', names='Status', title='Distribuição por Status',
                                 color='Status', color_discrete_map={'Concluída': '#00CC96', 'Em andamento': '#FFA15A', 'Em aberto': '#EF553B'})
                    fig.update_traces(textposition='inside', textinfo='percent+label', marker=dict(line=dict(color='#000000', width=1)))
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    st.write("#### Quantidade de OS por Prioridade e Status")
                    prioridade_counts = df_filtrado.groupby(['ie_prioridade', 'status']).size().reset_index(name='Quantidade')
                    fig = px.bar(prioridade_counts, x='ie_prioridade', y='Quantidade', color='status', barmode='group',
                                 title='Distribuição por Prioridade e Status', labels={'ie_prioridade': 'Prioridade', 'Quantidade': 'Quantidade de OS'},
                                 color_discrete_map={'Concluída': '#00CC96', 'Em andamento': '#FFA15A', 'Em aberto': '#EF553B'})
                    fig.update_layout(xaxis_title="Prioridade", yaxis_title="Quantidade de OS")
                    st.plotly_chart(fig, use_container_width=True)
            
            elif st.session_state.selected_view == "Tempo de Atendimento":
                col1, col2 = st.columns(2)
                with col1:
                    st.write("#### Tempo Médio de Atendimento por Prioridade")
                    df_concluidas = df_filtrado[df_filtrado['status'] == 'Concluída']
                    tempo_medio = df_concluidas.groupby('ie_prioridade')['tempo_atendimento'].mean().reset_index()
                    tempo_medio = tempo_medio.sort_values('tempo_atendimento', ascending=False)
                    if not tempo_medio.empty:
                        fig = px.bar(tempo_medio, x='ie_prioridade', y='tempo_atendimento', title='Tempo Médio de Atendimento por Prioridade (dias)',
                                     labels={'ie_prioridade': 'Prioridade', 'tempo_atendimento': 'Tempo (dias)'}, color='tempo_atendimento',
                                     color_continuous_scale=px.colors.sequential.Viridis)
                        fig.update_layout(xaxis_title="Prioridade", yaxis_title="Tempo Médio (dias)")
                        st.plotly_chart(fig, use_container_width=True)
                    else: st.info("Não há Ordens de Serviço Concluídas no período selecionado.")
                with col2:
                    st.write("#### Tempo Médio de Espera para Início por Prioridade")
                    df_iniciadas = df_filtrado[df_filtrado['status'].isin(['Em andamento', 'Concluída'])]
                    tempo_espera = df_iniciadas.groupby('ie_prioridade')['tempo_espera'].mean().reset_index()
                    tempo_espera = tempo_espera.sort_values('tempo_espera', ascending=False)
                    if not tempo_espera.empty:
                        fig = px.bar(tempo_espera, x='ie_prioridade', y='tempo_espera', title='Tempo Médio de Espera por Prioridade (dias)',
                                     labels={'ie_prioridade': 'Prioridade', 'tempo_espera': 'Tempo (dias)'}, color='tempo_espera',
                                     color_continuous_scale=px.colors.sequential.Viridis)
                        fig.update_layout(xaxis_title="Prioridade", yaxis_title="Tempo Médio (dias)")
                        st.plotly_chart(fig, use_container_width=True)
                    else: st.info("Não há Ordens de Serviço que iniciaram no período selecionado.")
            
            elif st.session_state.selected_view == "Solicitantes":
                st.write("#### Top 10 Solicitantes com Mais Ordens de Serviço")
                top_solicitantes = df_filtrado['nm_solicitante'].value_counts().reset_index()
                top_solicitantes.columns = ['Solicitante', 'Quantidade']
                top_solicitantes = top_solicitantes.head(10)
                if not top_solicitantes.empty:
                    fig = px.bar(top_solicitantes, x='Quantidade', y='Solicitante', title='Top 10 Solicitantes',
                                 orientation='h', color='Quantidade', color_continuous_scale=px.colors.sequential.Viridis)
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                else: st.info("Não há solicitantes com Ordens de Serviço no período selecionado.")

            elif st.session_state.selected_view == "Painel de Acompanhamento": # Chama a função dedicada para a nova aba
                exibir_painel_acompanhamento(df_filtrado)
            
            st.markdown("---") # Separador visual para as seções abaixo que não são controladas por abas

            # Linha do tempo das OS criadas (essas seções são sempre mostradas abaixo da visualização selecionada)
            st.header("Evolução Mensal das Ordens de Serviço por Status")
            st.write("Este gráfico mostra como o número de Ordens de Serviço em diferentes status (Concluídas, Em Andamento, Em Aberto) evoluiu ao longo do tempo, com base na data de criação.")
            
            df_timeline = df_filtrado.copy()
            df_timeline['mes_ano'] = df_timeline['dt_criacao'].dt.strftime('%Y-%m')
            timeline_data = df_timeline.groupby(['mes_ano', 'status']).size().reset_index(name='quantidade')
            timeline_data['mes_ano_dt'] = pd.to_datetime(timeline_data['mes_ano'] + '-01')
            timeline_data = timeline_data.sort_values('mes_ano_dt')
            
            if not timeline_data.empty:
                fig = px.line(timeline_data, x='mes_ano', y='quantidade', color='status', title='Evolução de OS por Mês',
                              labels={'mes_ano': 'Mês/Ano', 'quantidade': 'Quantidade de OS'}, markers=True,
                              color_discrete_map={'Concluída': '#00CC96', 'Em andamento': '#FFA15A', 'Em aberto': '#EF553B'})
                fig.update_layout(xaxis_title="Mês/Ano", yaxis_title="Quantidade de OS")
                st.plotly_chart(fig, use_container_width=True)
            else: st.info("Não há dados de linha do tempo para exibir com os filtros atuais.")
            
            st.markdown("---") 

            # Tabela de detalhamento das Ordens de Serviço
            st.header("Detalhamento das Ordens de Serviço Filtradas")
            st.write("Visualize todas as Ordens de Serviço que correspondem aos filtros selecionados, com informações detalhadas para cada uma.")
            
            colunas_exibir = ['nr_os', 'ds_solicitacao', 'nm_solicitante', 'ie_prioridade', 
                              'dt_criacao', 'dt_inicio', 'dt_termino', 'nm_responsavel', 'status']
            colunas_existentes = [col for col in colunas_exibir if col in df_filtrado.columns]
            df_exibir = df_filtrado[colunas_existentes].copy()
            
            colunas_renomeadas = {
                'nr_os': 'Nº OS', 'ds_solicitacao': 'Solicitação', 'nm_solicitante': 'Solicitante',
                'ie_prioridade': 'Prioridade', 'dt_criacao': 'Data Criação', 'dt_inicio': 'Data Início',
                'dt_termino': 'Data Término', 'nm_responsavel': 'Responsável', 'status': 'Status'
            }
            renomeacao = {k: v for k, v in colunas_renomeadas.items() if k in df_exibir.columns}
            df_exibir = df_exibir.rename(columns=renomeacao)
            
            for col in ['Data Criação', 'Data Início', 'Data Término']:
                if col in df_exibir.columns and df_exibir[col].dtype.kind == 'M': 
                    df_exibir[col] = df_exibir[col].dt.strftime('%d/%m/%Y %H:%M').fillna('N/A')
            
            st.dataframe(df_exibir) # use_container_width removido para compatibilidade
            
            st.markdown("---") 

            # Detalhes de uma Ordem de Serviço selecionada
            st.header("Detalhes da Ordem de Serviço Selecionada")
            st.write("Selecione uma Ordem de Serviço para ver todos os detalhes, incluindo descrições completas e tempos calculados.")
            
            if 'nr_os' in df_filtrado.columns and not df_filtrado['nr_os'].empty:
                os_selecionada_nr = st.selectbox("Selecione o Número da OS para ver detalhes:", sorted(df_filtrado['nr_os'].unique().tolist()))
                if os_selecionada_nr:
                    os_detalhes = df_filtrado[df_filtrado['nr_os'] == os_selecionada_nr].iloc[0]
                    col_left, col_right = st.columns(2)
                    with col_left:
                        st.subheader(f"OS #{os_detalhes['nr_os']}")
                        st.write(f"**Solicitação:** {os_detalhes['ds_solicitacao'] if pd.notna(os_detalhes['ds_solicitacao']) else 'N/A'}")
                        st.write(f"**Solicitante:** {os_detalhes['nm_solicitante'] if pd.notna(os_detalhes['nm_solicitante']) else 'N/A'}")
                        st.write(f"**Prioridade:** {os_detalhes['ie_prioridade'] if pd.notna(os_detalhes['ie_prioridade']) else 'N/A'}")
                        st.write(f"**Status:** {os_detalhes['status'] if pd.notna(os_detalhes['status']) else 'N/A'}")
                    with col_right:
                        st.write(f"**Data de Criação:** {os_detalhes['dt_criacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_criacao']) else 'N/A'}")
                        st.write(f"**Data de Início:** {os_detalhes['dt_inicio'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_inicio']) else 'N/A'}")
                        st.write(f"**Data de Término:** {os_detalhes['dt_termino'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_termino']) else 'N/A'}")
                        st.write(f"**Responsável:** {os_detalhes['nm_responsavel'] if pd.notna(os_detalhes['nm_responsavel']) else 'N/A'}")
                        st.write(f"**Última Atualização:** {os_detalhes['dt_ultima_atualizacao'].strftime('%d/%m/%Y %H:%M') if pd.notna(os_detalhes['dt_ultima_atualizacao']) else 'N/A'}")
                    
                    if 'ds_completa_servico' in os_detalhes and pd.notna(os_detalhes['ds_completa_servico']):
                        st.subheader("Descrição Completa da Ordem de Serviço")
                        st.write(os_detalhes['ds_completa_servico'])
                    else: st.subheader("Descrição Completa da Ordem de Serviço"); st.info("Sem descrição detalhada disponível para esta OS.")
                    
                    st.subheader("Métricas de Tempo da OS")
                    if pd.notna(os_detalhes['dt_criacao']) and pd.notna(os_detalhes['dt_inicio']):
                        tempo_espera = (os_detalhes['dt_inicio'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                        st.write(f"**Tempo de espera para início:** {tempo_espera:.2f} dias")
                    else: st.write("**Tempo de espera para início:** N/A (OS não iniciada ou sem data de criação/início)")
                    if pd.notna(os_detalhes['dt_criacao']) and pd.notna(os_detalhes['dt_termino']):
                        tempo_total = (os_detalhes['dt_termino'] - os_detalhes['dt_criacao']).total_seconds() / (24*60*60)
                        st.write(f"**Tempo total de atendimento:** {tempo_total:.2f} dias")
                    else: st.write("**Tempo total de atendimento:** N/A (OS não concluída ou sem data de criação/término)")
            else: st.info("Nenhuma Ordem de Serviço disponível para seleção ou a coluna 'nr_os' não foi encontrada.")
        
        # Pausa o script por 30 segundos antes da próxima re-execução completa
        time.sleep(30) 

# Ponto de entrada da aplicação Streamlit
if __name__ == "__main__":
    main()
