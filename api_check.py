import requests

def get_user_age(user_name):
    url = 'https://api.agify.io'
    response = requests.get(url,params={'name': user_name})
    jsn_response = response.json()
    return jsn_response['age']

age = get_user_age('Gangaraju')
print('the age of user is '+ str(age) )