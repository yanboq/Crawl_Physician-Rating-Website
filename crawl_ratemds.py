# -*- coding: UTF-8 -*-
import html5lib, requests
import mysql.connector
import re,time
from bs4 import BeautifulSoup


# database info
username = 'root'
password = ''
host = 'localhost'
dbase = 'doctor_rating'

class crawlRatemds:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password,host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_address(self):

        select_sql = (
        "SELECT cId,doctorNPI,searchResultUrl,searchKeyword FROM doctor_rating.google_results2017 WHERE websiteName = 'www.ratemds.com' AND crawlFlag = 0 LIMIT 1"
        )

        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():

            cId = cursor_data[0]
            doctor_NPI = cursor_data[1]
            doctor_url = cursor_data[2]

            print('---------New Url From Google----------')
            print(cId, doctor_NPI, doctor_url)

            try:
                doctor_id = doctor_url.split('/')[4]
                url_category = doctor_url.split('/')[3]
            except:
                print('---------Fault url----------')

            if_doctor_id = re.findall(r'\d+', doctor_id)
            try:
                    self.get_data(doctor_id, doctor_url, doctor_NPI, cId)
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName = 'www.ratemds.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName = 'www.ratemds.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
            except Exception as err:
                print("---------!!!Crawl Fail!!!----------")
                print(err)
                insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
                self.cursor.execute(insert_sql, (cId,))
                self.dbconn.commit()

                time.sleep(60)

    def get_data(self, doctor_id, doctor_url, doctor_NPI, cId):

        print("------------Doctor Information------------")

        wb_data = requests.get(doctor_url)
        soup = BeautifulSoup(wb_data.text,'html5lib')

        # soup = BeautifulSoup(open("C:/Users/Robin/Desktop/ratemds.html"),"html5lib")

        doctor_name = soup.select("div.doctor-banner.map-banner > div > div > div > div.col-sm-6 > h1")[0].get_text().strip()
        doctor_speciality = soup.select(".search-item-info")[0].get_text().strip()
        try:
            rating_overall = soup.select(".star-rating")[0]['title'].split(' ')[0]
            rating_number = int(soup.select("div.doctor-banner.map-banner > div > div > div > div.col-sm-6 > div > div > span > span")[0].get_text())
        except Exception:
            rating_overall = ""
            rating_number = ""
        try:
            doctor_ranking = soup.select(".search-item-info")[2].get_text().split('of')[0].split('#')[1].strip()
            doctor_rankingall = soup.select(".search-item-info")[2].get_text().split('of')[1].strip().split('\n')[0]
        except Exception:
            doctor_ranking = ""
            doctor_rankingall = ""

        try:
            doctor_gender = soup.select(".search-item-info")[3].get_text().strip()
            doctor_city = soup.select(".search-item-info")[2].get_text().split(' in')[1].split(',')[0].strip()
            doctor_state = soup.select(".search-item-info")[2].get_text().split(' in')[1].split(',')[1].strip()
        except Exception:
            doctor_gender = ""
            doctor_city = ""
            doctor_state = ""

        try:
            print ("Doctor information:", doctor_name, doctor_NPI, doctor_speciality, rating_overall, rating_number, doctor_ranking, doctor_rankingall, doctor_gender, doctor_city, doctor_state)
        except Exception:
            pass

        args = (doctor_id, doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_ranking, doctor_rankingall, doctor_gender, doctor_city, doctor_state)
        add_doctor_result = ("INSERT IGNORE INTO ratemds_doctor "
                             "(doctor_id, doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_ranking, doctor_rankingall, doctor_gender, doctor_city, doctor_state) "
                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        self.cursor.execute(add_doctor_result, args)
        self.dbconn.commit()

        if rating_number != "":

            insert_sql = (
            "UPDATE `google_results2017` SET ratingFlag = 1 WHERE cId = %s and websiteName = 'www.ratemds.com'")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()

            page_number = rating_number // 10 + 1
            print ("Review page:" + str(page_number))
            for i in range(1, page_number+1):
                page_url = "?page=" + str(i)
                doctor_page_url = doctor_url + page_url
                print (doctor_page_url)

                wb_data = requests.get(doctor_page_url)
                soup = BeautifulSoup(wb_data.text, 'html5lib')

                j = rating_number / 10

                if j > 0:

                    for k in range(0, 10):
                        for review_data in soup.select('span.ratings'):
                            try:
                                rating_score = review_data.select(".star-rating")[k]["title"]
                                rating_date = review_data.select('.link-plain')[k].get_text().split('Submitted ')[1]
                                rating_content = review_data.select('.rating-comment-body')[k].get_text().strip()
                                rating_metrics = review_data.select('.rating-numbers-compact')[k]
                                rating_staff = rating_metrics.select('.value')[0].get_text().strip()
                                rating_punctuality = rating_metrics.select('.value')[1].get_text().strip()
                                rating_helpfulness = rating_metrics.select('.value')[2].get_text().strip()
                                rating_knowledge = rating_metrics.select('.value')[3].get_text().strip()

                                print("------------Review------------")
                                print(rating_score, rating_date, rating_content,rating_staff,rating_punctuality,rating_helpfulness,rating_knowledge)

                                args = (doctor_id, doctor_NPI, rating_score, rating_date, rating_content, rating_staff,
                                        rating_punctuality, rating_helpfulness, rating_knowledge)
                                add_rating_result = ("INSERT IGNORE INTO ratemds_rating "
                                                     "(doctor_id, doctor_NPI, rating_score, rating_date, rating_content, rating_staff, rating_punctuality, rating_helpfulness, rating_knowledge) "
                                                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
                                self.cursor.execute(add_rating_result, args)

                                self.dbconn.commit()

                            except Exception:
                                pass

                    rating_number -= 10

                else:
                    for k in range(0, rating_number):
                        for review_data in soup.select('span.ratings'):
                            try:
                                # print review_data.prettify()
                                rating_score = review_data.select(".star-rating")[k]["title"]
                                rating_date = review_data.select('.link-plain')[k].get_text().split('Submitted ')[1]
                                rating_content = review_data.select('.rating-comment-body')[k].get_text().strip()
                                rating_metrics = review_data.select('.rating-numbers-compact')[k]
                                rating_staff = rating_metrics.select('.value')[0].get_text().strip()
                                rating_punctuality = rating_metrics.select('.value')[1].get_text().strip()
                                rating_helpfulness = rating_metrics.select('.value')[2].get_text().strip()
                                rating_knowledge = rating_metrics.select('.value')[3].get_text().strip()

                                print ("------------Review------------")
                                try:
                                    print (rating_score, rating_date, rating_content, rating_staff, rating_punctuality, rating_helpfulness, rating_knowledge)
                                except Exception:
                                    pass
                                args = (doctor_id, doctor_NPI, rating_score, rating_date, rating_content, rating_staff, rating_punctuality, rating_helpfulness, rating_knowledge)
                                add_rating_result = ("INSERT IGNORE INTO ratemds_rating "
                                                     "(doctor_id, doctor_NPI, rating_score, rating_date, rating_content, rating_staff, rating_punctuality, rating_helpfulness, rating_knowledge) "
                                                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
                                self.cursor.execute(add_rating_result, args)

                                self.dbconn.commit()

                            except Exception as err:
                                print(err)
    def get_task_number(self):
        select_sql = (
            "SELECT count(*) FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName = 'www.ratemds.com'"
        )
        self.cursor.execute(select_sql)
        for cursor_data in self.cursor.fetchall():
            task_number = int(cursor_data[0])
        print('The number of task this time: ', task_number)
        return task_number

    def main(self):
        task_number = self.get_task_number()
        for i in range(0, task_number):
            self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
            self.cursor = self.dbconn.cursor()
            self.get_address()
            self.cursor.close()
            self.dbconn.close()

if __name__ == '__main__':
    crawler = crawlRatemds()
    crawler.main()