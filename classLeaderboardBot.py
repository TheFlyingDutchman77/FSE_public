import time
import yaml
import json
import math
import requests
from discord_webhook import DiscordWebhook, DiscordEmbed
import csv
from filelock import Timeout, FileLock
import datetime

class LeaderboardBot():
    def __init__(self, customer, customer_full_name, image_url, user_key, webhook,servicekey):
        self.customer = customer
        self.customer_full_name = customer_full_name
        self.image_url = image_url
        self.user_key = user_key
        self.webhook = webhook
        self.servicekey = servicekey
        self.updateOK = False

    # Python code to sort the tuples using  element x[.]  
    # of sublist Inplace way to sort using sort() 
    def Sort(self, sub_li, key): 
    
        # reverse = None (Sorts in Ascending order) 
        # key is set to sort using 3rd element of  
        # sublist lambda has been used 
        sub_li.sort(key = lambda x: (x[key]), reverse = True) 
        return sub_li 

    def LoadFile(self, filename):
        data_file = list(csv.reader(open(filename,"r"), delimiter=","))
        #print('File opened for processing: ', filename)
        #Column definition in file: Id,Type,Time,Distance,Pilot,SerialNumber,Aircraft,MakeModel,From,To,TotalEngineTime,FlightTime,GroupName,Income,PilotFee,CrewCost,BookingFee,Bonus,FuelCost,GCF,RentalPrice,RentalType,RentalUnits,RentalCost,

        data_array = []
        for row in data_file:
            data_array.append(row)
            #old_group_log.append([row[0], row[1], row[2],row[3],row[4],row[5],row[6],row[7],row[8], row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23]])
        
        return data_array
                    
    # Get data from FSE
    def FSEupdate(self):
        print(f"Starting FSE update for LeaderboardBot for customer {self.customer_full_name}.")
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        csv_url = f"https://server.fseconomy.net/data?servicekey=SERVICEKEY&format=csv&query=flightlogs&search=monthyear&readaccesskey=USERKEY&month={str(month)}&year={str(year)}"
        csv_url = csv_url.replace('SERVICEKEY',self.servicekey)
        csv_url = csv_url.replace('USERKEY', self.user_key)
        
        try:
            lock = FileLock("fse.txt.lock",timeout=5)
            with lock:
                req = requests.get(csv_url)
                url_content = req.content
                if len(url_content) > 100:
                    csv_file = open(f"{self.customer}/group_flight_log_download_{self.customer}.csv", 'wb')
                    csv_file.write(url_content)
                    csv_file.close()
                self.updateOK = True
                
        except Exception as e:
            print(f"Error in FSE update cycle: {str(e)}")
            self.updateOK = False

        time.sleep(1)
        print(f"Finished FSE update for {self.customer_full_name} ")

    # Update discord data
    def update(self):
        
        if self.updateOK:
            try:
                print(f'Starting Leaderboard Bot cycle for customer {self.customer_full_name}')

                #delete last 2 empty rows in  file
                filename_old = f"{self.customer}/group_flight_log_download_{self.customer}.csv"
                filename_new = f"{self.customer}/group_flight_log_{self.customer}.csv"
                with open(filename_old) as old, open(filename_new, 'w') as new:
                    lines = old.readlines()
                    new.writelines(lines[1:-1])
                    
                #Open new flight log file for reading
                group_log_new_unsorted = self.LoadFile(f"{self.customer}/group_flight_log_{self.customer}.csv")
                group_log_new = self.Sort(group_log_new_unsorted,0) #Sort by id 
                                
                rownr = 0
                nrofflights = 0
                totaldistance = 0
                totalflighttime = 0
                totalhours = 0
                totalminutes = 0
                pilotstats = []
                emailContent=''
                for row in group_log_new: 
                    nrofflights = nrofflights + 1
                    totaldistance = totaldistance + int(row[3])
                    flighttime = row[11].split(":")
                    hours = int(flighttime[0])
                    minutes = int(flighttime[1])
                    totalhours = totalhours + hours
                    totalminutes = totalminutes + minutes
                    pilot = row[4]
                    newpilot = True
                    for i in range(len(pilotstats)):
                        if pilotstats[i][0] == pilot:
                            newpilot = False
                            pilot_index = i
                    if newpilot == False:
                        pilotstats[pilot_index][1] = pilotstats[pilot_index][1] + 1
                        pilotstats[pilot_index][2] = pilotstats[pilot_index][2] + int(row[3])
                        pilotstats[pilot_index][3] = pilotstats[pilot_index][3] + hours
                        pilotstats[pilot_index][4] = pilotstats[pilot_index][4] + minutes
                    else:
                        pilot_nrofflights = 1
                        distance = int(row[3])
                        flighttime = row[11].split(":")
                        hours = int(flighttime[0])
                        minutes = int(flighttime[1])
                        pilotstats.append([pilot, pilot_nrofflights, distance, hours, minutes])
                        
                #Calultate total flight times
                minutestohours = math.floor(totalminutes/60)
                totalhours = totalhours + minutestohours
                minutesleft = totalminutes - minutestohours*60
                
                #Calculate pilot flight times
                for j in range(len(pilotstats)):
                    pilot_minutestohours = math.floor(pilotstats[j][4]/60)
                    pilotstats[j][3] = pilotstats[j][3] + pilot_minutestohours
                    pilot_minutesleft = pilotstats[j][4] - pilot_minutestohours*60
                    pilotstats[j][4] = pilot_minutesleft
                    
                
                pilotstats_sorted = self.Sort(pilotstats,1)
                
                if nrofflights > 0:

                    emailContent = "**Group statistics for this month** \n\n" + "Number of flights: "+ str(nrofflights) + '\n' + 'Distance flown: ' + str(totaldistance) + ' nm' + '\n' + 'Flight time: ' + str(totalhours) + 'hrs and ' + str(minutesleft) + 'min \n\n\n' + '**Pilot stats:** \n\n'
                    for k in range(len(pilotstats)):
                        if pilotstats[k][1] > 1: # if more than one flight
                            emailContent = emailContent + '*Pilot: ' + str(pilotstats[k][0]) + '*\n' + str(pilotstats[k][1]) + ' flights, ' + str(pilotstats[k][2]) + ' nm, ' + str(pilotstats[k][3]) + ' hrs and ' + str(pilotstats[k][4]) + ' min \n\n'
                        else:
                            emailContent = emailContent + '*Pilot: ' + str(pilotstats[k][0]) + '*\n' + str(pilotstats[k][1]) + ' flight, ' + str(pilotstats[k][2]) + ' nm, ' + str(pilotstats[k][3]) + ' hrs and ' + str(pilotstats[k][4]) + ' min \n\n'
                    
                
                    mUrl = self.webhook
                    webhook = DiscordWebhook(url=mUrl)
                    embed = DiscordEmbed(
                        title="Daily leaderboard", description= emailContent, color=242424
                    )
                    embed.set_thumbnail(
                        url = self.image_url,
                    )
                    webhook.add_embed(embed)
                    response = webhook.execute()
                        
                    #print(pilotstats)
                    
                    succes = True
                
                time.sleep(1)
                print("Finished!")
            

                
            except Exception as e:
                print("Stopped due to exception: ", e)
                emailContent = f"Error occured in cycle of Leaderboard Bot: {self.customer} with error {str(e)}"
                
          
        print(f"Program cycle finished for Leaderboard Bot for customer {self.customer}")
        
