import httpx
from bs4 import BeautifulSoup
from collections import defaultdict


class HonestSignChecker:
    def __init__(self):
        self.api_key = ''  # API-ключ


    def get_token_crpt(self):
        # Получение токена для авторизации через API.
        url = "https://appglapi.1mark.ru:5566/ping_auth"
        headers = {
            'Authorization': self.api_key
        }
        response = httpx.get(url, headers=headers)
        response.raise_for_status()  # Проверка на ошибки
        
        return response.json().get("token")  # Получаем токен
    
    
    def check_code(self, code):
        # Проверка кода маркировки
        token = self.get_token_crpt()
        url = "https://markirovka.crpt.ru/api/v3/true-api/mods/info"  # URL для проверки
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "cis": code,  # Код для проверки вутри функции
            "pg": ["beer"]
        }
        
        try:
            response = httpx.post(url, json=payload, headers=headers)
            response.raise_for_status()  # Проверка
            return response.json()
        
        except httpx.HTTPStatusError as e:
            print(f"Ошибка HTTP-запроса: {e.response.status_code} - {e.response.text}")
        
        except Exception as e:
            print(f"Другая ошибка: {e}")


if __name__ == "__main__":
    results = defaultdict(list)  # Словарь, где ключ — gtin, значение — коды


    checker = HonestSignChecker()
    
    with open("input.XML", "r", encoding="utf8") as input_file:
        soup = BeautifulSoup(input_file, features='xml')
        names = soup.find_all('catESAD_cu:IdentifacationMeansUnitCharacterValueId')
        
        for index_of_ki, name in enumerate(names):
            #print(name.get_text())
            new_gtin = str(name.get_text())[2:16]
            code_to_check = str(name)

            results[new_gtin].append(str(name.get_text()))
            #print(results)
            
            result_code = checker.check_code(code_to_check)
            if result_code:
                print("Результат проверки кода:", result_code)

            result_gtin = checker.check_code(new_gtin)
            if result_gtin:
                print("Результат проверки gtin:", result_gtin)