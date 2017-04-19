# -*- coding: UTF-8 -*-
import html5lib, requests
import mysql.connector
import re
import time
from bs4 import BeautifulSoup

# database info
username = 'root'
password = ''
host = 'localhost'
dbase = 'doctor_rating'



class crawlYelp:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password,host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_address(self):

        select_sql = (
        "SELECT cId,doctorNPI,searchResultUrl,searchKeyword FROM doctor_rating.google_results2017 WHERE websiteName LIKE '%yelp.com' AND crawlFlag = 0 LIMIT 1"
        )
        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():

            cId = cursor_data[0]
            doctor_NPI = cursor_data[1]
            doctor_url = cursor_data[2]
            doctor_keyword = cursor_data[3]

            print('---------New Url From Google----------')
            print(cId, doctor_NPI, doctor_url, doctor_keyword)

            if doctor_url.split('/')[3] == 'biz':
                try:

                    print('---------Valid url----------')
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%yelp.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%yelp.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
                    self.get_data(doctor_url, doctor_NPI, cId)



                except Exception as err:
                    print("---------!!!Crawl Fail!!!----------")
                    print(err)
                    insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
                    self.cursor.execute(insert_sql, (cId,))
                    self.dbconn.commit()

                    time.sleep(2)
            else:
                print("---------Not valid url----------")


    def get_data(self,doctor_url, doctor_NPI, cId):
        print("------------Doctor Information------------")

        wb_data = requests.get(doctor_url)
        soup = BeautifulSoup(wb_data.text,'html5lib')

        if soup.select('.alert-message')[0].get_text().strip().split(' ')[0] == 'Yelpers':
            print('Has closed')
            rating_number = ''
            insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()
        else:

            doctor_name = soup.select("div.biz-page-header-left > div > h1")[0].get_text().split(',')[0].strip()

            try:
                doctor_speciality = soup.select("div.biz-main-info.embossed-text-white > div.price-category > span > a")[0].get_text().split(',')[0].strip()
            except:
                doctor_speciality = ''

            try:
                rating_overall = soup.select("div.biz-main-info.embossed-text-white > div.rating-info.clearfix > div.biz-rating.biz-rating-very-large.clearfix > div")[0]['title'].split(' ')[0]

                rating_number = soup.select("div.biz-main-info.embossed-text-white > div.rating-info.clearfix > div.biz-rating.biz-rating-very-large.clearfix > span")[0].get_text().strip().split(' ')[0]
            except IndexError:
                rating_overall = ''
                rating_number = ''


            doctor_address_data = soup.select("div > strong > address")[0].get_text()
            doctor_address = ' '.join(doctor_address_data.split())

            try:

                doctor_street = doctor_address.split(',')[0].split(' ')[-1]
                doctor_state = doctor_address.split(',')[1].split(' ')[1]
                doctor_zipcode = doctor_address.split(',')[1].split(' ')[2]
            except:
                doctor_street = ''
                doctor_state = ''
                doctor_zipcode = ''

            try:
                print("Doctor Information:", doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_street, doctor_state, doctor_zipcode)
            except Exception:
                pass
            args = (doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_street, doctor_state, doctor_zipcode)
            add_doctor_result = ("INSERT IGNORE INTO yelp_doctor "
                                  "(doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_street, doctor_state, doctor_zipcode) "
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self.cursor.execute(add_doctor_result, args)
            self.dbconn.commit()

        if rating_number != '':

            insert_sql = (
            "UPDATE `google_results2017` SET ratingFlag = 1  WHERE cId = %s and websiteName LIKE '%yelp.com'")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()

            page_number = int(rating_number) // 20 + 1
            print ("Review page:" + str(page_number))
            for i in range(0, page_number):
                page_url = "?start=" + str(20 * i)
                doctor_page_url = doctor_url + page_url
                print (doctor_page_url)

                wb_data = requests.get(doctor_page_url)
                soup = BeautifulSoup(wb_data.text, 'html5lib')

                for review_data in soup.select(".review--with-sidebar"):
                    try:
                        # print review_data
                        reviewer_id = review_data.select('#dropdown_user-name')[0]['href'].split('=')[1]
                        reviewer_name = review_data.select('#dropdown_user-name')[0].get_text().strip()
                        reviewer_address = review_data.select('.user-location')[0].get_text().strip()

                        try:
                            reviewer_city = review_data.select('.user-location')[0].get_text().strip().split(',')[1]
                            reviewer_state = review_data.select('.user-location')[0].get_text().strip().split(',')[2].strip()
                        except:
                            reviewer_city = review_data.select('.user-location')[0].get_text().strip().split(',')[0]
                            reviewer_state = review_data.select('.user-location')[0].get_text().strip().split(',')[1].strip()


                        rating_score = review_data.select('.i-stars')[0]['title'].split(' ')[0]
                        rating_date = review_data.select('.rating-qualifier')[0].get_text().strip().split(' ')[0].strip()
                        rating_content = review_data.select('p')[0].get_text().strip()
                        rating_useful = review_data.select('.count')[0].get_text().strip()
                        rating_funny = review_data.select('.count')[1].get_text().strip()
                        rating_cool = review_data.select('.count')[2].get_text().strip()


                        print ("------------Review------------")
                        try:
                            print (reviewer_id, reviewer_name, reviewer_address, reviewer_city, reviewer_state, rating_score, rating_date, rating_content, rating_useful, rating_cool, rating_funny)
                        except Exception:
                            pass

                        args = (doctor_NPI, reviewer_id, reviewer_name, reviewer_address, reviewer_city, reviewer_state, rating_score, rating_date, rating_content, rating_useful, rating_cool, rating_funny)
                        add_rating_result = ("INSERT IGNORE INTO yelp_rating "
                                              "(doctor_NPI, reviewer_id, reviewer_name, reviewer_address, reviewer_city, reviewer_state, rating_score, rating_date, rating_content, rating_useful, rating_cool, rating_funny) "
                                              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                        self.cursor.execute(add_rating_result, args)
                        self.dbconn.commit()


                    except Exception as err:
                        pass

    def get_task_number(self):
        select_sql = (
            "SELECT count(*) FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName LIKE '%yelp.com'"
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
    crawler = crawlYelp()
    crawler.main()
