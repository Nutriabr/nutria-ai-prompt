import pandas as pd
import hashlib
import random
from database.conexao import engine

# CRIANDO FUNÇOES DE TRATAMENTO
def pseudonimizar(valor): 
    if pd.isna(valor): 
        return None
    salt = str(random.randint(1000, 9999))
    return hashlib.sha256((str(valor) + salt ).encode()).hexdigest()

def anonimizar_email(valor):
    if pd.isna(valor):
        return None
    return "xxxx@xxxx.xxxx"

def anonimizar_senha(valor):
    if pd.isna(valor):
        return None
    return hashlib.sha256(str(valor).encode()).hexdigest()

def generalizar_tel (valor):
    if pd.isna(valor):
        return None
    valor = str(valor)
    return "xxxxxx" + valor[-2:]

def anonimizar_emp (valor):
    if pd.isna(valor):
        return None
    return f"EMP_{random.randint(1000,9999)}"

def anonimizar_foto (valor): 
    return "anonimo.png"

def anonimizar_simples(valor, substituto="xxxx"):
    if pd.isna(valor):
        return None
    return substituto

# Inicializa conexao como None para o bloco 'finally'
conexao = None

# Verifica se a 'engine' foi importada e criada com sucesso
if engine:
    try:
        # FAZENDO A CONEXAO COM O BD
        conexao = engine.connect() 
        print("Conexão com BD estabelecida!")

        # COLETANDO DOS DADOS
        queryUsuario = "SELECT * FROM usuario"
        queryAdmin = "SELECT * FROM admin"

        resultUsuario = conexao.exec_driver_sql(queryUsuario)
        resultAdmin = conexao.exec_driver_sql(queryAdmin)

        df_usuario = pd.DataFrame(resultUsuario.fetchall(), columns=resultUsuario.keys())
        df_admin = pd.DataFrame(resultAdmin.fetchall(), columns=resultAdmin.keys())
        
        df_usuario["origem"] = "usuário"
        df_admin["origem"] = "admin"

        df_total = pd.concat([df_usuario, df_admin], ignore_index=True, sort=False)

        df_tratado = pd.DataFrame()
        df_tratado["id"] = df_total["id"]
        df_tratado["origem"] = df_total["origem"]

        # APLICANDO AS FUNÇÔES NOS CAMPOS
        # TABELA USUÁRIO
        if "nome" in df_total.columns:
            df_tratado["nome_original"] = df_total["nome"]
            df_tratado["nome_pseudonimo"] = df_total["nome"].apply(pseudonimizar)

        if "email" in df_total.columns:
            df_tratado["email_original"] = df_total["email"]
            df_tratado["email_anonimo"] = df_total["email"].apply(anonimizar_email)

        if "senha" in df_total.columns:
            df_tratado["senha_original"] = df_total["senha"]
            df_tratado["senha_hash"] = df_total["senha"].apply(anonimizar_senha)

        if "telefone" in df_total.columns:
            df_tratado["telefone_original"] = df_total["telefone"]
            df_tratado["telefone_generalizado"] = df_total["telefone"].apply(generalizar_tel)

        if "empresa" in df_total.columns:
            df_tratado["empresa_original"] = df_total["empresa"]
            df_tratado["empresa_pseudonimo"] = df_total["empresa"].apply(anonimizar_emp)

        if "foto" in df_total.columns:
            df_tratado["foto_original"] = df_total["foto"]
            df_tratado["foto_anonima"] = df_total["foto"].apply(anonimizar_foto)

        # TABELA ADMIN
        if "nascimento" in df_total.columns:
            df_tratado["nascimento_original"] = df_total["nascimento"]
            df_tratado["nascimento_anonimo"] = df_total["nascimento"].apply(lambda x: "0000-00-00")

        if "cargo" in df_total.columns:
            df_tratado["cargo_original"] = df_total["cargo"]
            df_tratado["cargo_anonimo"] = df_total["cargo"].apply(anonimizar_simples)

        # EXPORTANDO PARA JSON
        df_tratado.to_json('./output/dados_tratados.json', orient='records', force_ascii=False, indent=4)

        print("Arquivo JSON criado com sucesso: dados_tratados.json")

        # EXPORTANDO PARA EXCEL 
        df_tratado.to_excel('./output/dados_tratados.xlsx', sheet_name='DadosSensiveis', index=False)
        print("Arquivo EXCEL criado com sucesso: dados_tratados.xlsx")

        # EXIBE AS PRIMEIRAS TRES LINHAS DA PLANILHA dados_tratados.xlsx
        df_lido = pd.read_excel('./output/dados_tratados.xlsx', sheet_name='DadosSensiveis')
        print(df_lido.head())

    except Exception as erro:
        print(f"Erro durante o processamento: {erro}")

    finally:
        # ENCERRANDO CONEXÃO COM O BD
        if conexao:
            conexao.close()
            print("Conexão encerrada.")
        else:
            print("Conexão não foi estabelecida, nada a fechar.")
else:
    print("Erro: A engine do banco de dados não foi criada ou importada.")
    print("Verifique o arquivo 'CONEXAO.py' e seus erro.")