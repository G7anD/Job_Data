import datetime as dt
from bs4 import BeautifulSoup
import requests
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float
import sys

def progress(count, total, suffix=''):
    bar_len = 40
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()

Base = declarative_base()

class Jobs(Base):
    __tablename__ = 'jobs_'
    id = Column(Integer, primary_key=True)
    j_id = Column( Integer)
    province = Column(String)
    region = Column(String)
    specialization = Column(String)
    enterprise = Column(String)
    inn = Column(Integer)
    salary = Column(Integer)
    rate = Column(Float)
    free_places = Column(Integer)
    published = Column(Date)

    def __repr__(self):
        return "<Jobs(j_id='{}', province='{}', rate='{}', salary={}, published={})>"\
                .format(self.j_id, self.province, self.rate, self.salary, self.published)

class PageData:
    def __init__(self, url):
        self.url = url
        self.content = self.simple_get()
        self.collect_page_data()

    def simple_get(self):
        """making http get request"""

        try:
            with closing(get(self.url, stream=True)) as resp:
                if self.is_good_response(resp):
                    return resp.content
                else:
                    return None

        except RequestException as e:
            self.log_error(
                'Error during requests to {0} : {1}'.format(self.url, str(e)))
            return None

    def is_good_response(self, resp):
        """ checking response status code """

        content_type = resp.headers['Content-Type'].lower()
        return (resp.status_code == 200
                and content_type is not None
                and content_type.find('html') > -1)

    def log_error(self, e):
        """ printing error type """

        print(e)

    def variables_cost(self, list):
        """ defining variable types """

        list[0] = int(list[0])
        list[5] = int(list[5])
        sl = list[6].split(',')[0].split('.')
        salary = ''

        for i in sl:
            salary+=i

        list[6] = int(salary)
        list[7] = float(list[7])
        list[8] = int(list[8])
        list[9] = dt.datetime.strptime(list[9], '%d.%m.%Y').date()
        return list

    def collect_page_data(self):
        """ getting full data from one page """

        bhtml = BeautifulSoup(self.content, 'html.parser')
        tds = bhtml.find('tbody')
        items = tds.find_all("tr")

        self.data_lists = []

        for tr in items:
            sub_list = []
            for td in tr.find_all("td"):
                sub_list.append(td.text)
            del sub_list[-1]
            sub_list = self.variables_cost(sub_list)
            self.data_lists.append(sub_list)

        return self.data_lists

    def print_it(self):
        """ printing page data """

        for raw in self.data_lists:
            print(raw)

class SiteData:
    def __init__(self, page_number, url):
        self.page_num = page_number
        self.url_def = url
        self.collect_site_data()

    def collect_site_data(self):
        """ collecting all of site data """

        self.all_data = []
        for page in range(1, self.page_num+1):
            url = self.url_def + "?dp-1-page={}".format(page)
            data_page = PageData(url)
            self.all_data.extend(data_page.collect_page_data())
            progress(page-1, self.page_num+1, 'collecting')

        print("\nIn total: {} case data were collected.".format(len(self.all_data)))
        return self.all_data

    def export_to_csv(self):
        cols = ['#', 'Вилоят', 'Туман(шахар)', 'Мутахассислик', 'Корхона номи', 'СТИР(ИНН)', 'Маош', 'Ставка', 'Бўш иш ўринлари сони', 'Вакансия эълон қилинган сана']

        self.df = pd.DataFrame(self.all_data, columns=cols)
        self.df.to_csv('job_data.csv')
        print(self.df.describe())
        return self.df


class Database:
    auth = 'postgres:pass@localhost:5432'
    DATABASE_URI = 'postgres+psycopg2://{}/job_data'.format(auth)
    engine = create_engine(DATABASE_URI)

    def __init__(self):
        self.connection = self.engine.connect()
        print("db instance created")

    def create_table(self):
        Base.metadata.create_all(self.engine)

    def save(self, data):
        session = Session(bind=self.connection)
        for i in data:
            jdata = Jobs(
                j_id = i[0],
                province = i[1],
                region = i[2],
                specialization = i[3],
                enterprise = i[4],
                inn = i[5],
                salary = i[6],
                rate = i[7],
                free_places = i[8],
                published = i[9]
                )
            session.add(jdata)
            session.commit()
        session.close()

url = "http://ish.mehnat.uz/vacancy/index"

# page_one = PageData(url)
# page_one.print_it()

data = SiteData(page_number=10, url=url)
data.export_to_csv()  # creating csv data file
db = Database() # connecting to database
db.create_table() # creating table
db.save(data.all_data) # saving to database
