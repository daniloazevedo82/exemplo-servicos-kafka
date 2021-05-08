from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import json

PROCESSO = "separacao"
PROCESSO_ANTERIOR = "pagamento"
ESTOQUE = 1

def iniciar():
    global offset
    offset = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
    cliente.add_topic(PROCESSO)
    cliente.close()


def recuperar_processo_anterior(processo):
    anterior = None
    for item in processo["processos"]:
        if item["processo"] == PROCESSO_ANTERIOR:
            anterior = item
    return anterior


def processo_anterior_ok(processo):
    ok = False

    if processo is not None:
        ok = (processo["sucesso"] == 1)

    return ok


def executar():
    global offset
    resultado = "ok"

    # recupera resultados do processo anterior
    consumidor = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        api_version=(0, 10, 1))
    particao = TopicPartition(PROCESSO_ANTERIOR, 0)
    consumidor.assign([particao])
    consumidor.seek(particao, offset)

    for processo in consumidor:
        # incrementa o offset para "andar" na lista de mensagens na proxima chamada
        offset = processo.offset + 1

        processo = processo.value
        processo = json.loads(processo)

        # verifica se o processo anterior ocorreu com sucesso
        processo_anterior = recuperar_processo_anterior(processo)
        if processo_anterior_ok(processo_anterior):
            sucesso = 1
            erro_servico = ""
            if processo_anterior["dados"]["quantidade"] > ESTOQUE:
                erro_servico = "Em falta no estoque"
                resultado = erro_servico
                sucesso = 0

            # incrementa o log do processo
            processo["processos"].append({
                "processo": PROCESSO,
                "sucesso": sucesso,
                "erro": erro_servico,
                "dados": processo_anterior["dados"]
            })
            try:
                produtor = KafkaProducer(
                    bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
                produtor.send(topic=PROCESSO, value=json.dumps(
                    processo).encode("utf-8"))
            except KafkaError as erro:
                resultado = f"erro: {erro}"

    # o certo eh imprimir em (ou enviar para) um log de resultados
    print(resultado)


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
