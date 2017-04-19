# -*- coding: UTF-8 -*-
import html5lib, requests
import mysql.connector
import random
import time
import re
from bs4 import BeautifulSoup

# database info
username = 'root'
password = ''
host = 'localhost'
dbase = 'doctor_rating'


class crawlHealthgrades:
    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()

    def get_address(self):


        select_sql = (
        "SELECT cId,doctorNPI,searchResultUrl,searchKeyword FROM doctor_rating.google_results2017 WHERE websiteName = 'www.healthgrades.com' AND crawlflag = 0 LIMIT 1"
        )

        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():

            print('---------New Url From Google----------')

            cId = cursor_data[0]
            doctor_NPI = cursor_data[1]
            doctor_url = cursor_data[2]
            doctor_keyword = cursor_data[3]

            print(cId, doctor_NPI, doctor_url)

            url_category = doctor_url.split('/')[3]

            try:

                print('------------Valid url-------------')
                insert_sql = (
                "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName = 'www.healthgrades.com' and cId != %s and crawlflag = 0")
                insert_sql1 = (
                "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName = 'www.healthgrades.com' and cId = %s")
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

                time.sleep(6)

    def get_data(self, doctor_url, doctor_NPI, cId):

        print("------------Doctor Information------------")

        wb_data = requests.get(doctor_url)

        soup = BeautifulSoup(wb_data.text,'html5lib')

        doctor_name = soup.find('h1', itemprop="name").get_text().split(',')[0].strip()
        print(doctor_name)
        try:
            doctor_speciality = soup.select(".provider-specialty")[0].get_text().split('|')[0].strip()
        except:
            doctor_speciality = soup.select(".provider-details")[0].get_text().split('|')[0].strip()

        try:
            doctor_gender = soup.select(".provider-specialty")[0].get_text().split('|')[1].strip()
        except:
            doctor_gender = soup.select(".provider-details")[0].get_text().split('|')[1].strip()


        try:
            rating_overall = soup.select('.stars-explanation')[0].get_text().split('is ')[1].split(' out')[0]
            rating_number = soup.select('.review-count')[0].get_text().split(' ')[0]
        except:
            rating_overall = ""
            rating_number = ""

        try:
            doctor_street = soup.find('p', itemprop="streetAddress").get_text()
        except:
            try:
                doctor_street = soup.find('span', itemprop="streetAddress").get_text()
            except Exception as err:
                doctor_street = ""

        doctor_city = soup.find('span', itemprop="addressLocality").get_text()
        doctor_state = soup.find('span', itemprop="addressRegion").get_text()
        doctor_zipcode = soup.find('span', itemprop="postalCode").get_text()

        doctor_address = " ".join((doctor_street, doctor_city, doctor_state, doctor_zipcode))


        try:
            print ("Doctor information:", doctor_name, doctor_speciality, rating_overall, rating_number,doctor_gender, doctor_zipcode, doctor_city, doctor_state, doctor_address)
        except Exception:
            pass

        doctor_exps = soup.select('.overlay-container .tab-container')

        doctor_allspecialties = ""
        doctor_conditions_treated = ""
        doctor_procedures_performed = ""
        doctor_education = ""
        doctor_languages = ""
        doctor_board = ""
        for doctor_exp in doctor_exps:

            doctor_exp_name = doctor_exp.select('h2')[0].get_text()

            if doctor_exp_name == "Specialties":
                for doctor_tempdata in doctor_exp.select('li'):
                    doctor_allspecialties += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Conditions Treated":
                for doctor_tempdata in doctor_exp.select('li'):
                    doctor_conditions_treated += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Procedures Performed":
                for doctor_tempdata in doctor_exp.select('li'):
                    doctor_procedures_performed += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Education":
                for doctor_tempdata in doctor_exp.select('li'):
                    doctor_education += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Languages Spoken":
                for doctor_tempdata in doctor_exp.select('li'):
                    doctor_languages += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Board Certifications":
                for doctor_tempdata in doctor_exp.select('div.hg3-striped-list > ul > li'):
                    doctor_board += doctor_tempdata.get_text() + '|'
            if doctor_exp_name == "Background Check":
                doctor_tempdata1 = doctor_exp.select('.hg3-overlay-summary')[0]
                malpractice_claims_num = doctor_tempdata1.select('.hg3-coin')[0].get_text().strip()
                try:
                    malpractice_claims_content = doctor_tempdata1.select('.background-section')[0].get_text().strip()
                except:
                    malpractice_claims_content = ''


                doctor_tempdata2 = doctor_exp.select('.hg3-overlay-summary')[1]
                sanctions_num = doctor_tempdata2.select('.hg3-coin')[0].get_text().strip()
                try:
                    sanctions_content = doctor_tempdata2.select('.background-section')[0].get_text().strip()
                except:
                    sanctions_content = ''


                doctor_tempdata3 = doctor_exp.select('.hg3-overlay-summary')[2]
                board_actions_num = doctor_tempdata3.select('.hg3-coin')[0].get_text().strip()
                try:
                    board_actions_content = doctor_tempdata3.select('.background-section')[0].get_text().strip()
                except:
                    board_actions_content = ''

            # almost every doctor don't have these

            # if doctor_exp_name == "Memberships & Professional Affiliations":
            #     print('ok')
            #     for doctor_tempdata in doctor_exp.select('li'):
            #         doctor_memberships= doctor_tempdata.get_text() + '|'
            #         print(doctor_memberships)
            # if doctor_exp_name == "Awards & Recognition":
            #     print('ok')
            #     for doctor_tempdata in doctor_exp.select('div > ul > li'):
            #         doctor_awards= doctor_tempdata.get_text() + '|'
            #         print(doctor_awards)





        try:
            rating_trustworthiness = round(float(soup.select('.filled-percentage')[0]['style'].split(':')[1].split('%')[0]) / 100 * 5, 1)
            rating_expalains = round(float(soup.select('.filled-percentage')[1]['style'].split(':')[1].split('%')[0]) / 100 * 5, 1)
            rating_answers = round(float(soup.select('.filled-percentage')[2]['style'].split(':')[1].split('%')[0]) / 100 * 5, 1)
            rating_timespent = round(float(soup.select('.filled-percentage')[3]['style'].split(':')[1].split('%')[0]) / 100 * 5, 1)
        except:
            rating_trustworthiness = ""
            rating_expalains = ""
            rating_answers = ""
            rating_timespent = ""


        args = (
        doctor_NPI,doctor_name, doctor_speciality, rating_overall, rating_number, doctor_gender, doctor_zipcode, doctor_city,
        doctor_state, doctor_address,doctor_allspecialties, doctor_conditions_treated,
        doctor_procedures_performed, doctor_education, doctor_languages, doctor_board, malpractice_claims_num,
        malpractice_claims_content, sanctions_num, sanctions_content, board_actions_num, board_actions_content,
        rating_trustworthiness, rating_expalains, rating_answers,rating_timespent)
        add_doctor_result = ("INSERT IGNORE INTO healthgrades_doctor"
                             "(doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number,doctor_gender, doctor_zipcode, doctor_city, doctor_state, doctor_address, doctor_allspecialties, doctor_conditions_treated,doctor_procedures_performed, doctor_education, doctor_languages,doctor_board,malpractice_claims_num, malpractice_claims_content,sanctions_num,sanctions_content,board_actions_num,board_actions_content,rating_trustworthiness, rating_expalains, rating_answers,rating_timespent)"
                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s)")
        self.cursor.execute(add_doctor_result, args)
        self.dbconn.commit()

        if rating_number != "":

            insert_sql = (
            "UPDATE `google_results2017` SET ratingFlag = 1 WHERE cId = %s and websiteName = 'www.healthgrades.com'")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()

            for review_tempdata in soup.find_all('div', itemprop='review'):

                print("------------Review------------")

                review_infodata = review_tempdata.select('.commenter-info')[0].get_text().strip()
                try:
                    reviewer_name = review_infodata.split('-')[1].split(' | ')[0].split(' in')[0]
                except:
                    reviewer_name = ''

                if len(review_infodata) > 12:
                    try:
                        reviewer_city = review_infodata.split('in ')[1].split(' |')[0].split(',')[0]
                    except:
                        if review_infodata[0] != '-':
                            try:
                                reviewer_city = review_infodata.split(' |')[0].split(',')[0]
                            except:
                                reviewer_city = ''
                        else:
                            reviewer_city = ''
                    try:
                        reviewer_state = review_infodata.split('in ')[1].split(' |')[0].split(',')[1]
                    except:
                        try:
                            reviewer_state = review_infodata.split(' |')[0].split(',')[1]
                        except:
                            reviewer_state = ''
                else:
                    reviewer_city = ''
                    reviewer_state = ''
                try:
                    rating_date = review_infodata.split('| ')[1].strip()
                except:
                    try:
                        rating_date = review_infodata
                    except:
                        rating_date = ''
                rating_content = review_tempdata.select('.comment-text')[0].get_text().strip()
                rating_score = review_tempdata.find('span', itemprop="ratingValue").get_text()

                try:
                    print(reviewer_name, reviewer_city, reviewer_state, rating_score, rating_date, rating_content)
                except Exception:
                    pass
                args = (doctor_NPI, reviewer_name, reviewer_city, reviewer_state, rating_score, rating_date, rating_content)
                add_rating_result = ("INSERT IGNORE INTO healthgrades_rating "
                                     "(doctor_NPI, reviewer_name, reviewer_city, reviewer_state, rating_score, rating_date, rating_content) "
                                     "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                self.cursor.execute(add_rating_result, args)

                self.dbconn.commit()


        else:
            print("---------No Review--------")

    def get_task_number(self):
        select_sql = (
            "SELECT count(*) FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName = 'www.healthgrades.com'"
        )
        self.cursor.execute(select_sql)
        for cursor_data in self.cursor.fetchall():
            task_number = int(cursor_data[0])
        print('The number of task in this time: ', task_number)
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
    crawler = crawlHealthgrades()
    crawler.main()