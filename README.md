# dxpq_ext

Este projeto é uma extensão em C para Python, localizada no diretório `dxpq_ext`. Ele inclui um arquivo `dxpq_ext.c` que pode ser compilado e utilizado em conjunto com o Python. O objetivo é aprender sobre a interação entre Python e C, e explorar a criação de extensões customizadas.


## Como executar o Projeto

Clone o repositório:
```bash
git clone git@github.com:pedrohsbarbosa99/dxpq_ext.git
```

Entre na pasta do projeto:
```bash
cd dxpq_ext
```

Instale as dependências do PostgreSQL:
```bash
sudo apt-get install -y gcc build-essential libpq-dev python3-dev
```

Instale as dependências de Dev:
```bash
pip install -r requirements-dev.txt
```

Buildar a biblioteca `dxpq`:
```bash
./build.sh
```

## Exemplos

```python
import dxpq_ext

conn = dxpq_ext.PGConnection("conninfo")
cursor = dxpq_ext.PGCursor(conn)
cursor.execute("SELECT * FROM table")

for row in cursor.fetchall():
    print(row)
```