import time
import yaml
import json
import requests
from discord_webhook import DiscordWebhook, DiscordEmbed
import csv
from filelock import Timeout, FileLock


class NewAircraftScannerRegionBot():
    def __init__(self, customer, customer_full_name, image_url, webhook, servicekey):
        self.customer = customer
        self.customer_full_name = customer_full_name
        self.image_url = image_url
        self.webhook = webhook
        self.servicekey = servicekey
        self.updateOK = False

        config_file = f"{self.customer}/config_new_aircraft_scanner_bot_{self.customer}.yml"
        with open(config_file,'r') as file:
            self.var_config = yaml.full_load(file)

    # Python code to sort the tuples using second element  
    # of sublist Inplace way to sort using sort() 
    def Sort(self, sub_li): 
    
        # reverse = None (Sorts in Ascending order) 
        # key is set to sort using 3rd element of  
        # sublist lambda has been used 
        sub_li.sort(key = lambda x: (x[1],x[7])) 
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
        print(f"Starting new aircraft scanner FSE update for customer {self.customer_full_name}")
        csv_url = "https://server.fseconomy.net/data?servicekey=SERVICEKEY&format=csv&query=aircraft&search=forsale"
        csv_url = csv_url.replace('SERVICEKEY',self.servicekey)
                
        try:
            lock = FileLock("fse.txt.lock",timeout=5)
            with lock:
                req = requests.get(csv_url)
                url_content = req.content
                if len(url_content) > 100:
                    csv_file = open(f"{self.customer}/all_aircraft_for sale_download_{self.customer}.csv", 'wb')
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
        print(f"Starting new aircraft scanner update cycle for customer {self.customer_full_name}")
        if self.updateOK:
            try:
                prices =[]
        
                #Read current aircraft in region from prices.txt file
                location = [] # empty array to store actual locations
                ac_names = []
                prices = []
                ids = []
                pricesfilename = f"{self.customer}/prices_{self.customer}.txt"
                prices_file = open(pricesfilename, "r")
                line = prices_file.readline() #skip first line
                line = prices_file.readline()
                while line != "END\n":
                    temp = line.split(':',1)
                    ac_names.append(temp[0])
                    temp1 = temp[1].split(';')
                    prices.append(int(temp1[0]))
                    ids.append(temp1[1])
                    location.append(temp1[2].rstrip('\n')) 
                    line = prices_file.readline()
                
                #print(location)
                    
                #delete last 2 empty rows in  file
                filename_old = f"{self.customer}/all_aircraft_for sale_download_{self.customer}.csv"
                filename_new = f"{self.customer}/all_aircraft_for sale_{self.customer}.csv"
                with open(filename_old) as old, open(filename_new, 'w') as new:
                    lines = old.readlines()
                    new.writelines(lines[1:-1])        

                #Read historical price data
                ac_names_avg = []
                price_avg = []
                quantity_avg = []
                previous_prices = []

                old_avg_data = list(csv.reader(open('aircraft_sales_avg.txt',"r"), delimiter=","))
                
                for row in old_avg_data :
                    ac_names_avg.append(row[0])
                    price_avg.append(row[1])
                    quantity_avg.append(row[2])
                    previous_prices.append([row[3],row[4],row[5],row[6],row[7]])
                
                aircraft_fs_temp = list(csv.reader(open(f"{self.customer}/all_aircraft_for sale_{self.customer}.csv","r"), delimiter=","))
                #print('File opened for processing')
                #SerialNumber,MakeModel,Registration,Owner,Location,LocationName,Home,SalePrice,SellbackPrice,Equipment,RentalDry,RentalWet,RentalType,Bonus,RentalTime,RentedBy,PctFuel,NeedsRepair,AirframeTime,EngineTime,TimeLast100hr,LeasedFrom,MonthlyFee,FeeOwed,
                
                rownr = 0
                aircraft_fs = []
                for row in aircraft_fs_temp:
                    aircraft_fs.append([row[0], row[1], row[2],row[3],row[4],row[5],row[6],float(row[7]),float(row[8]), row[9], row[18]])
                    rownr = rownr+1
                    
                price_sorted_aircraft_fs = self.Sort(aircraft_fs)


                new_location = [] # empty array to store actual locations
                new_ac_names = []
                new_prices = []
                new_ids = []
                

                # Output sorted csv file
                with open(f"{self.customer}/all_aircraft_for sale_sorted_{self.customer}.csv", 'w') as result_file:
                    emailContent = ''
                    if len(price_sorted_aircraft_fs) > 100:
                        for k in range(len(price_sorted_aircraft_fs)):
                            if price_sorted_aircraft_fs[k][4][0] == "Y": #Aircraft for sale is located in Australia
                                #print('Aircraft found in Australi at ', price_sorted_aircraft_fs[k][4], price_sorted_aircraft_fs[k][1]) 
                                new_prices.append(price_sorted_aircraft_fs[k][7])
                                new_ids.append(price_sorted_aircraft_fs[k][0])
                                new_location.append(price_sorted_aircraft_fs[k][4])
                                new_ac_names.append(price_sorted_aircraft_fs[k][1])
                                
                                prijs = f"{price_sorted_aircraft_fs[k][7]:,}" 
                                temp = prijs.split('.',1)
                                prijs = temp[0]
                                
                                #cycle through all aircraft in prices.txt from former cycle to check if we have a new aircraft
                                ac_new  = True
                                for l in range(len(ids)):
                                    if price_sorted_aircraft_fs[k][0] == ids[l]:
                                        ac_new = False
                                
                                if ac_new:
                                    print('It is a new aircraft for  sale!')
                                    emailContent = emailContent + f"New aircraft found in your region: \nType: {price_sorted_aircraft_fs[k][1]} \nLocation: {price_sorted_aircraft_fs[k][4]} \nPrice: {prijs} v$ \n"
  

                        if emailContent != '':
                            mUrl = self.webhook
                            webhook = DiscordWebhook(url=mUrl)
                            embed = DiscordEmbed(
                                title="New aircraft found", description= emailContent, color=242424
                            )
                            embed.set_thumbnail(
                                url = self.image_url,
                            )
                            webhook.add_embed(embed)
                            response = webhook.execute()
                            print('Following message sent to Discord:')
                            print(emailContent)
                            print("Sent!")
                        else:
                            print('Nothing to send')
                            
                result_file.close()
        
            except Exception as e:
                print("Stopped due to exception: ", e)
                emailContent = f"Error occured in cycle of NewAircraftScannerRegionBot: {self.customer} with error {str(e)}"
            
            finally:
                with open(f"{self.customer}/prices_{self.customer}.txt", 'w') as prices_file:
                    prices_file.write('# List aircraft with lowest prices found so far'+'\n')
                    for i in range(len(new_prices)):
                        string = new_ac_names[i]+': '+ str(int(new_prices[i]))+';' + new_ids[i]+';' + new_location[i]+'\n'
                        prices_file.write(string)
                    string = 'END' + '\n'
                    prices_file.write(string)
                prices_file.close()
          
        print(f"Update cycle finished for NewAircraftScannerBot for customer {self.customer}")
        
