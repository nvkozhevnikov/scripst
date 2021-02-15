import requests
from bs4 import BeautifulSoup
import re
import mysql.connector
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os


from_email = os.environ['EMAIL_FOR_NOTIFICATION']
password = os.environ['EMAIL_FOR_NOTIFICATION_PASSWORD']
to_email = os.environ['EMAIL_FOR_NOTIFICATION_TO_STB']
subject_mail = 'Links report'


def connect_db ():

    config = {
    'user': os.environ['DB_TRUSTHOST_LINKS_CHECKER_USER'],
    'password': os.environ['DB_TRUSTHOST_LINKS_CHECKER_PASS'],
    'host': os.environ['DB_TRUSTHOST_HOST'],
    'database': os.environ['DB_TRUSTHOST_LINKS_CHECKER_DB_NAME'],
    'raise_on_warnings': True,
    'use_pure': True
    }

    cnx = mysql.connector.connect(**config)
    return cnx


def analysis_links():
    cnx = connect_db()
    cursor = cnx.cursor()

    query = "SELECT a.donor_url, b.date, b.donor_link_status, b.donor_href_rel_status, b.donor_meta_robots_status, " \
            "b.acceptor_http_status_code FROM noykana_links_checker.links a, noykana_links_checker.checks_table b " \
            "WHERE b.date = (SELECT date FROM noykana_links_checker.checks_table ORDER BY date DESC LIMIT 1) AND " \
            "a.id IN (SELECT donor_id FROM noykana_links_checker.checks_table WHERE donor_link_status='Not available' " \
            "OR donor_href_rel_status='nofollow' OR donor_meta_robots_status='noindex') AND a.id = b.donor_id"

    cursor.execute(query)
    row = cursor.fetchall()
    cursor.close()
    cnx.close()
    return row


def send_data_mysql(http_code_donor, link_available, check_page_robots, http_code_acceptor):
    cnx = connect_db()
    cursor = cnx.cursor()

    query = ("INSERT INTO checks_table (donor_id, date, donor_http_status_code, donor_link_status, "
             "donor_meta_robots_status, donor_href_rel_status, acceptor_http_status_code) VALUES (%(donor_id)s, "
             "%(date)s, %(donor_http_status_code)s, %(donor_link_status)s, %(donor_meta_robots_status)s, "
             "%(donor_href_rel_status)s, %(acceptor_http_status_code)s);")

    insert_data = {
        'donor_id': link_available[0]['donor_id'],
        'date': date.today(),
        'donor_http_status_code': http_code_donor,
        'donor_link_status': link_available[0]['href_link_status'],
        'donor_meta_robots_status': check_page_robots,
        'donor_href_rel_status': link_available[0]['rel_link'],
        'acceptor_http_status_code': http_code_acceptor
    }

    cursor.execute(query, insert_data)
    cnx.commit()
    cursor.close()
    cnx.close()


def send_error_mysql(donor_id, ex):
    cnx = connect_db()
    cursor = cnx.cursor()

    query = "INSERT INTO checks_table (donor_id, date, error_status) VALUES (%(donor_id)s, %(date)s, %(error_status)s)"

    insert_data = {
        'donor_id': donor_id,
        'date': date.today(),
        'error_status': ex
    }
    print(insert_data)

    cursor.execute(query, insert_data)
    cnx.commit()
    cursor.close()
    cnx.close()


def data_for_analysis():
    cnx = connect_db()
    cursor = cnx.cursor()
    query = ("SELECT id, donor_url, acceptor_url FROM links WHERE deletion_date IS NULL")
    cursor.execute(query)
    row = cursor.fetchall()
    cursor.close()
    cnx.close()
    return row


def check_page(result):
    soup = BeautifulSoup(result.text)
    check_page_data = 'Data not available'
    for robots in soup.findAll('meta', attrs={'name': re.compile('robots')}):
        if type(robots.get('content')) == str:
            check_page_data = robots.get('content')
    return check_page_data


def check_url_available(result, url, donor_id):
    soup = BeautifulSoup(result.text)
    links = []
    for link in soup.findAll('a', attrs={'href': re.compile("^https://sterbrust")}):

        # Parse the parameter value "href="
        href_link = link.get('href')
        if url == href_link:
            href_link_status = 'Available'
        else:
            href_link_status = 'Not available'

        # Parse the parameter value "rel="
        rel_data = link.get('rel')
        NoneType = type(None)
        if type(rel_data) == NoneType:
            rel_link = 'Not available'
        else:
            rel_link = link.get('rel')[0]

        # Parse the anchor link
        anchor = link.text

        mass = {'donor_id': donor_id, 'href_link_status': href_link_status, 'rel_link': rel_link, 'anchor': anchor}
        links.append(mass)

    return links


def http_status_code(result):
    status_code = result.status_code
    return status_code


def response_conveyor(links):

    for url in links:
        try:
            donor_id = url[0]
            donor_url = url[1]
            acceptor_url = url[2]
            response_donor = requests.get(donor_url)
            http_code_donor = http_status_code(response_donor)
            link_available = check_url_available(response_donor, acceptor_url, donor_id)
            check_page_robots = check_page(response_donor)
            response_acceptor = requests.get(acceptor_url)
            http_code_acceptor = http_status_code(response_acceptor)
            send_data_mysql (http_code_donor, link_available, check_page_robots, http_code_acceptor)
            print(http_code_donor, link_available, check_page_robots, http_code_acceptor)
        except Exception as ex:
            try:
                send_error_mysql(donor_id, 'error')
                print(ex)

            except Exception as ex2:
                print(ex2)


def links_report(data):
    header_table = '<style>table {table-layout: fixed;width:100%} td {word-wrap:break-word;}</style>' \
                   '<table border="1" cellpadding="1" cellspacing="1"><tbody>'
    footer_table = '<tbody></table>'
    titles = '<tr><th rowspan="2">#</th><th rowspan="2">URL</th><th rowspan="2">Date</th><th colspan="4">Statuses</th></tr>' \
             '<tr><th>Donor Link</th><th>Href Rel</th><th>Meta Robots</th><th>Acceptor HTTP code</th></tr>'

    td_table = []
    for e, td in enumerate(data, start=1):
        tr1 = '<tr>'
        num = f'<td>{e}</td>'
        url = f'<td>{td[0]}</td>'
        date = f'<td>{td[1]}</td>'
        donor_link_status = f'<td>{td[2]}</td>'
        donor_href_rel_status = f'<td>{td[3]}</td>'
        donor_meta_robots_status = f'<td>{td[4]}</td>'
        acceptor_http_status_code = f'<td>{td[5]}</td>'
        tr2 = '</tr>'
        all = tr1 + num + url + date + donor_link_status + donor_href_rel_status + donor_meta_robots_status + \
              acceptor_http_status_code + tr2
        td_table.append(all)

    td_table.insert(0, header_table)
    td_table.insert(1, titles)
    td_table.append(footer_table)
    result = ''.join(td_table)

    send_mail(from_email, password, to_email, subject_mail, result)


def send_mail(from_email, password, to_email, subject, message) -> object:
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    msg.attach(MIMEText(f'<h1 style="color: red">{subject}</h1><div>{message}</div>', 'html'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()


if __name__ == '__main__':
    links = data_for_analysis()
    response_conveyor(links)
    data = analysis_links()
    links_report(data)






