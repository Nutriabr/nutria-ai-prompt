import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

db_dialect = os.getenv("Dialect")
db_driver = os.getenv("Driver")
db_user = os.getenv("User")
db_password = os.getenv("Password")
db_host = os.getenv("Host")
db_port = os.getenv("Port")
db_name = os.getenv("Database_name")

database_url = f"{db_dialect}+{db_driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = None

try:
    engine = create_engine(database_url)
    print("Conexão criada com sucesso!")

except Exception as e:
    print(f"Erro ao conectar: {e}")