import time
import yaml
import json
import requests
from discord_webhook import DiscordWebhook, DiscordEmbed
import csv
from filelock import Timeout, FileLock

class FlightLogBot():
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
        sub_li.sort(key = lambda x: (x[key])) 
        return sub_li 

    # Function to load file and set in array
    def LoadFile(self, filename):
        data_file = list(csv.reader(open(filename,"r"), delimiter=","))
        #Column definition in file: Id,Type,Time,Distance,Pilot,SerialNumber,Aircraft,MakeModel,From,To,TotalEngineTime,FlightTime,GroupName,Income,PilotFee,CrewCost,BookingFee,Bonus,FuelCost,GCF,RentalPrice,RentalType,RentalUnits,RentalCost,

        data_array = []
        for row in data_file:
            data_array.append(row)
            #old_group_log.append([row[0], row[1], row[2],row[3],row[4],row[5],row[6],row[7],row[8], row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23]])
        
        return data_array
                
    # Get data from FSE
    def FSEupdate(self):
        print(f"Starting FSE update for FlightLogBot for customer {self.customer_full_name}.")
        
        csv_url = "https://server.fseconomy.net/data?servicekey=SERVICEKEY&format=csv&query=flightlogs&search=id&readaccesskey=USERKEY&fromid=FLIGHTID"
        csv_url = csv_url.replace('SERVICEKEY',self.servicekey)
        csv_url = csv_url.replace('USERKEY', self.user_key)
        with open(f"{self.customer}/config_flight_log_bot_{self.customer}.yml",'r') as file:
            cust_config = yaml.full_load(file)   
        csv_url = csv_url.replace('FLIGHTID',str(cust_config['FROMID']-1))
        
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
        with open(f"{self.customer}/config_flight_log_bot_{self.customer}.yml",'r') as file:
            var_config = yaml.full_load(file)

        if self.updateOK:
            try:
                print(f'Starting flight log cycle for customer {self.customer_full_name}')

                new_log_found = False

                #delete last 2 empty rows in  file
                filename_old = f"{self.customer}/group_flight_log_download_{self.customer}.csv"
                filename_new = f"{self.customer}/group_flight_log_{self.customer}.csv"
                with open(filename_old) as old, open(filename_new, 'w') as new:
                    lines = old.readlines()
                    new.writelines(lines[1:-1])        
            
                #Open new flight log file for reading
                loadfile = f"{self.customer}/group_flight_log_{self.customer}.csv"
                group_log_new_unsorted = self.LoadFile(loadfile)
                group_log_new = self.Sort(group_log_new_unsorted,0) #Sort by id 
                
                #Ga hier verder
                emailContent=''
                for row in group_log_new: #Every flight log in the new file will be checked to see if it is newer than the last FROM ID
                    if int(row[0]) > int(var_config['FROMID']):
                        new_log_found = True
                        var_config['FROMID'] = int(row[0])
                        print('New flight log entry found')
                        paidtogroup = float(row[13])-float(row[14])-float(row[15])-float(row[16])-float(row[17])-float(row[18])-float(row[19])-float(row[23])
                        income ="{:,.2f}".format(float(row[13]))
                        paidtopilot = "{:,.2f}".format(float(row[14]))
                        paidtogroup = "{:,.2f}".format(paidtogroup)
                        emailContent = "**Flight log**" + "\n\n" + "Logged: "+ row[2] + "\n" + "Pilot: "+ row[4] + "\n" + "From ICAO: " + row[8] + "\n" + "To ICAO: " + row[9] + "\n" + "Distance: " + row[3] + " nm \n" + "Aircraft registration: " + row[6]+ "\n" + "Aircraft type: " + row[7] + '\n\n' + 'Income: v$ ' + str(income) + '\n' + 'Paid to pilot: v$ ' + str(paidtopilot) + '\n' + 'Paid to group: v$ '+ str(paidtogroup)
                        
                        #Exception for customer Karl
                        if self.customer == 'Karl':
                            emailContent = row[6] + " - " + row[7] + "\n" + row[3] + "\n" + row[11] + '\n' + 'Income: v$ ' + str(income) + '\n' + 'Paid to pilot: v$ ' + str(paidtopilot) + '\n' + 'Paid to group: v$ '+ str(paidtogroup)
                    
                        #Prepare discord message    
                        mUrl = self.webhook
                        webhook = DiscordWebhook(url=mUrl)
                        embed = DiscordEmbed(
                            title=row[8] + " - " + row[9], description= emailContent, color=242424
                        )
                        if self.customer != "Karl": # Karl has no logo
                            embed.set_thumbnail(
                                url = self.image_url,
                            )
                        webhook.add_embed(embed)
                        response = webhook.execute()
                        
                        time.sleep(0.5)
                        
                with open(f"{self.customer}/config_flight_log_bot_{self.customer}.yml", 'w') as file:
                    documents = yaml.dump(var_config, file)
                
                if new_log_found == False:
                    print('Nothing new to report')
                    
                time.sleep(1)
                print("Finished!")


                
            except Exception as e:
                print("Stopped due to exception: ", e)
                emailContent = f"Error occured in cycle of FlightLogBot: {self.customer} with error {str(e)}"
                
          
        print(f"Program cycle finished for FlightLogBot customer {self.customer}")
        
