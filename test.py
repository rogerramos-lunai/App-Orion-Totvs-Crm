import requests

url = "http://18.229.163.5:5678/webhook-test/test-webhook"
payload = {"mensagem": "Teste do webhook"}

r = requests.get(url, params=payload)  # ou requests.post se o webhook aceitar POST
print(r.status_code, r.text)
