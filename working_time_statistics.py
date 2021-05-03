import requests
from datetime import date

import config
import mysql_queries



def last_month_calculation():
    current_year = date.today().strftime('%Y')
    today = date.today().strftime('%Y-%m-%d')
    dates = [today, current_year]
    return dates


def working_time_statistics(db, dates):
    query_part = "SELECT emp.surname, ROUND((sum(late_time_day)/3600),2), ROUND((sum(early_end_time_day)/3600),2) " \
            "FROM noykana_payment_stb.crocotime_stb as crm " \
            "INNER JOIN noykana_payment_stb.employees_stb as emp ON emp.id = crm.id_employee " \
            f"WHERE date BETWEEN '{dates[1]}-01-01' AND '{dates[0]}' " \
            f"AND crm.id_employee IN ({config.MANAGERS_ID_CROCOTIME}) GROUP BY surname;"
    query_full = "SELECT emp.surname, count(summary_time_day) FROM noykana_payment_stb.crocotime_stb as crm " \
                 "INNER JOIN noykana_payment_stb.employees_stb as emp ON emp.id = crm.id_employee " \
                 f"WHERE date BETWEEN '{dates[1]}-01-01' AND '{dates[0]}' AND crm.id_employee " \
                 f"IN ({config.MANAGERS_ID_CROCOTIME}) AND summary_time_day = 0 GROUP BY surname"
    request_part = db.mysql_query(query_part)
    request_full = db.mysql_query(query_full)

    result = []
    for manager in request_part:
        for surname in request_full:
            if surname[0] == manager[0]:
                 data = {
                     ' Опоздания': manager[1],
                     'Ранний уход': manager[2],
                     'Использованные выходные дни': surname[1]
                 }
        all = {manager[0]:data}
        result.append(all)
    return result


def prepare_report(data):
    mass = []
    for iterac in data:
        for name, v in iterac.items():
            mass2 = []
            mass2.append(name)
            for i, j in v.items():
                pattern = i + ":" + " " + str(j)
                mass2.append(pattern)
            mass.append(mass2)

    v = len(mass)
    i = 0
    all_message = []
    while i < v:
        message = f'<b>{mass[i][0]}:</b> \n' \
                  f'\t {mass[i][1]} ч, \n ' \
                  f'\t {mass[i][2]} ч, \n ' \
                  f'\t {mass[i][3]} \n\n'
        all_message.append(message)
        i += 1
    current_year = date.today().strftime('%Y')
    header = f'<b>Статистика нерабочего времени с начала {current_year} года</b>\n\n'
    first_element = header + all_message.pop(0)
    all_message.insert(0, first_element)
    return all_message


def send_message_to_telegram(message_report):
    message_report = ' '.join(message_report)
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'parse_mode': 'html',
        'text': message_report
    }
    url = f'https://api.telegram.org/bot{config.TOKEN_TELEGRAM_BOT}/sendMessage'
    requests.post(url, data=payload)


if __name__ == '__main__':
    db_stb = mysql_queries.MySQLQueries(**config.DB_STB_PARAMS)
    dates = last_month_calculation()
    data = working_time_statistics(db_stb, dates)
    all_message = prepare_report(data)
    send_message_to_telegram(all_message)