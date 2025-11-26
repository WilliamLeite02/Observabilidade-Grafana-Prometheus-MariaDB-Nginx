import time
import random
import mysql.connector
import os

# Configurações via variáveis de ambiente
DB_HOST = os.getenv('DB_HOST', 'mariadb')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'senha123')
DB_NAME = os.getenv('DB_NAME', 'observabilidade')

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Espera o banco subir (retry logic simples)
print("Aguardando o banco subir...")
time.sleep(15) 

# Cria a tabela de teste
try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendas (
            id INT AUTO_INCREMENT PRIMARY KEY,
            produto VARCHAR(50),
            valor DECIMAL(10, 2),
            data TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print("Tabela criada com sucesso!")
except Exception as e:
    print(f"Erro ao criar tabela: {e}")

# Loop infinito gerando carga
produtos = ['Celular', 'Notebook', 'Mouse', 'Teclado', 'Monitor']
while True:
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. Inserir dado (Write)
        produto = random.choice(produtos)
        valor = round(random.uniform(50.0, 5000.0), 2)
        cursor.execute("INSERT INTO vendas (produto, valor) VALUES (%s, %s)", (produto, valor))

        # 2. Ler dados (Read)
        cursor.execute("SELECT count(*) FROM vendas")
        cursor.fetchall()

        conn.commit()
        conn.close()
        print(f"Venda registrada: {produto} - R$ {valor}")

        # Espera aleatória entre 0.1s e 1s para variar a carga
        time.sleep(random.uniform(0.1, 1.0))

    except Exception as e:
        print(f"Erro na operação: {e}")
        time.sleep(5)
