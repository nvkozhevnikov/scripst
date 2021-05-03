import requests
from datetime import date

import config
import mysql_queries


def last_month_calculation():
    current_month = date.today().strftime('%m')
    current_year = date.today().strftime('%Y')
    today = date.today().strftime('%Y-%m-%d')
    return today, current_month, current_year

def query_last_month_report(db, today, current_month, current_year):
    query = "SELECT  emp.surname, sum(quantity_of_leads) as ql, sum(total_amount_of_leads) as tal, " \
            "sum(quantity_of_orders) as qo, sum(total_amount_of_orders) as tao, " \
            "sum(quantity_of_sellings) as qs, sum(total_amount_of_sellings) as tas " \
            "FROM noykana_payment_stb.1c_crm_stb as crm " \
            "INNER JOIN noykana_payment_stb.employees_stb as emp ON emp.id = crm.id_employee " \
            f"WHERE date between '{current_year}-{current_month}-01' and '{today}' AND crm.id_employee IN (1,2,3) " \
            "group by surname;"
    request = db.mysql_query(query)

    report_for_telegram = "<b>Результаты работы с начала месяца: </b> \n\n" \
                          f"<b>{request[0][0]}</b>: лидов: {request[0][1]} на сумму: {'{0:,}'.format(request[0][2]).replace(',', ' ')} руб, " \
                          f"заказов: {request[0][3]} на сумму: {'{0:,}'.format(request[0][4]).replace(',', ' ')} руб, " \
                          f"реализаций: {request[0][5]} на сумму: {'{0:,}'.format(request[0][6]).replace(',', ' ')} руб. \n\n" \
                          f"<b>{request[1][0]}</b>: лидов: {request[1][1]} на сумму: {'{0:,}'.format(request[1][2]).replace(',', ' ')} руб, " \
                          f"заказов: {request[1][3]} на сумму: {'{0:,}'.format(request[1][4]).replace(',', ' ')} руб, " \
                          f"реализаций: {request[1][5]} на сумму: {'{0:,}'.format(request[1][6]).replace(',', ' ')} руб.\n\n" \
                          f"<b>{request[2][0]}</b>: лидов: {request[2][1]} на сумму: {'{0:,}'.format(request[2][2]).replace(',', ' ')} руб, " \
                          f"заказов: {request[2][3]} на сумму: {'{0:,}'.format(request[2][4]).replace(',', ' ')} руб, " \
                          f"реализаций: {request[2][5]} на сумму: {'{0:,}'.format(request[2][6]).replace(',', ' ')} руб.\n\n"

    return report_for_telegram

def send_message_to_telegram(message_report):
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'parse_mode': 'html',
        'text': message_report
    }
    url = f'https://api.telegram.org/bot{config.TOKEN_TELEGRAM_BOT}/sendMessage'
    requests.post(url, data=payload)

if __name__ == '__main__':
    dates = last_month_calculation()
    db_stb = mysql_queries.MySQLQueries(**config.DB_STB_PARAMS)
    report = query_last_month_report(db_stb, dates[0], dates[1], dates[2])
    send_message_to_telegram(report)

