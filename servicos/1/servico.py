from flask import Flask, jsonify, request

from kafka import KafkaClient, KafkaProducer
from kafka.errors import KafkaError

from time import sleep

import hashlib
import random
import string
import json

PROCESSO = "carrinho"
ESTOQUE = 10

def iniciar_topico():
    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
    cliente.add_topic(PROCESSO)
    cliente.close()


servico = Flask(__name__)

# Modelo json
# {
#     "produto": "Gift Card",
#     "quantidade": 1,
#     "valor": "100,00",
#     "cartão": "123456789",
#     "email": "cliente@cliente.com"
# }

def verificar_estoque(dados):
    ok = "ok"

    if dados['quantidade'] > ESTOQUE:
        ok = ok and "O produto selecionado não possui estoque."
    
    return ok


@servico.route("/executar", methods=["POST"])
def executar():
    resultado = "ok"
    sucesso = 1

    dados = request.get_json()

    # gerando um hash aleatorio para representar o ID de um processo
    ID = "".join(random.choice(string.ascii_letters + string.punctuation)
                 for x in range(12))
    ID = hashlib.md5(ID.encode("utf-8")).hexdigest()

    resultado = verificar_estoque(dados)

    erro_servico = ""
    if resultado != "ok":
        erro_servico = resultado
        sucesso = 0

    try:
        produtor = KafkaProducer(
            bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))

        jsonified = json.dumps({
            "identificacao": ID,
            "processos": [
                {
                    "processo": PROCESSO,
                    "sucesso": sucesso,
                    "erro": erro_servico,
                    "dados": dados,
                }
            ]
        }).encode("utf-8")
        produtor.send(topic=PROCESSO, value=jsonified)
    except KafkaError as erro:
        resultado = f"erro: {erro}"

    return resultado


if __name__ == "__main__":
    iniciar_topico()

    servico.run(
        host="0.0.0.0",
        debug=True
    )
