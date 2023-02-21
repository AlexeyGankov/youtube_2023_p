import sys
import time
import glob
#import csv
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
import pandas as pd
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
###
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
###
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
# from selenium.webdriver.chrome.options import Options
###

options = webdriver.ChromeOptions()
conn_string = os.environ['PG_DB']

def get_url(url, t_delay):
    options.add_argument(r"--user-data-dir=C:\python3\User Data")
    options.add_argument(r"--profile-directory=Profile 1")
    driver = webdriver.Chrome(executable_path=r'C:\python3\chromedriver.exe', chrome_options=options)
    time.sleep(3)
    driver.get("https://popsters.ru/app/dashboard")
# Let the user actually see something!
    driver.set_window_size(1643, 933)
    #driver.find_element(By.CSS_SELECTOR, ".search-field").click() ##### css=.offer > img
    time.sleep(5)
    driver.find_element(By.CSS_SELECTOR, ".search-field").click()
    driver.find_element(By.CSS_SELECTOR, ".search-field").send_keys(url)
    time.sleep(5)
    driver.find_element(By.CSS_SELECTOR, ".search-tool").click()
    time.sleep(5)
    driver.find_element(By.CSS_SELECTOR, ".close:nth-child(3)").click()
    time.sleep(5)
    driver.find_element(By.CSS_SELECTOR, ".app-button").click()
    time.sleep(5)
    element_present = expected_conditions.presence_of_element_located((By.CSS_SELECTOR, 'button:nth-child(3) > .export'))
    WebDriverWait(driver, t_delay).until(element_present)
    driver.implicitly_wait(t_delay)
    print("\n Element found!!!\n")
    time.sleep(5)
    driver.find_element(By.CSS_SELECTOR, "button:nth-child(3) > .export").click()
    time.sleep(5)
    driver.quit()


def main(filename):
	urls = []
	if len(filename) > 0:
		print("work with file ", filename)
		fp = open(filename, 'r')
		lines = fp.readlines()
		#Зебра в клеточку официальный ка;https://www.youtube.com/channel/UC4ozyeewhEmjvhH_1Q_yS4g;https://www.youtube.com/channel/UC4ozyeewhEmjvhH_1Q_yS4g;Мультфильмы;;1

		for lll in lines:
			line_parts = lll.split(";")
			urls.append([line_parts[2], "failed", "none", line_parts[0], line_parts[3].rstrip()])
		fp.close()
	else:
		print("work with sql : yt_channels")
		db = create_engine(conn_string)
		conn = db.connect()
		yt_ch = pd.read_sql('SELECT * FROM  yt_channels where flag_closed = 0', conn)
		lines = yt_ch.values.tolist()
		for lll in lines:
			urls.append([lll[1], "failed", "none", lll[0], lll[2], lll[4]])
		#'Зебра в клеточку официальный ка', 'https://www.youtube.com/channel/UC4ozyeewhEmjvhH_1Q_yS4g', 'Мультфильмы', 1, 'UC4ozyeewhEmjvhH_1Q_yS4g'
		conn.close()
	print(urls)
	url_statuses = [lll[1] for lll in urls]
	print(url_statuses.count("failed"))
	print(len(urls))
	list_old=glob.glob("./data/*.xlsx")
	t_delay = 90
	while url_statuses.count("failed") > 0 and t_delay < 150:
		for url_index in range(len(urls)):
			if urls[url_index][1] == "failed":
				line = urls[url_index][0]
		#		print(line.rstrip())
				try:
					get_url(line.rstrip(), t_delay)
					urls[url_index][1] = "OK"
					list_new = glob.glob("./data/*.xlsx")
					#print( list_new )
					#print( list_old )
					diff = list(set(list_new) - set(list_old))
					print(diff)
					if ( len(diff)> 0 ):
						urls[url_index][2] = diff[0].replace("\\", '/')
						list_old = list_new
				except: 
					print("Error downloading from: ", line.rstrip())
					urls[url_index][1] = "failed"
		url_statuses = [lll[1] for lll in urls]
		#print(url_statuses.count("failed"))
		t_delay += 35
# +++;+;+;+;
# https://www.youtube.com/channel/UC4ozyeewhEmjvhH_1Q_yS4g;OK;./data/posts.xlsx;Зебра в клеточку официальный ка;Мультфильмы;UC4ozyeewhEmjvhH_1Q_yS4g;	
	fp_error = open("report_urls.csv", 'a')
	fp_error.write("+++;+;+;+;\n")
	for lll in urls:
		fp_error.write(lll[0].rstrip()+";"+lll[1]+";"+lll[2]+";"+lll[3]+";"+lll[4]+";"+lll[5]+";\n")

	fp_error.close()
	fp_failed = open("failed_yt.csv", 'w')
	for lll in urls:
		if lll[1] == 'failed':
			fp_failed.write(lll[3].rstrip()+";"+lll[0]+";"+lll[0]+";"+lll[4]+";\n")
	fp_failed.close()
	
	
if len(sys.argv)>1:
	main(sys.argv[1])
else:
	main("")

