#Copyright 2012 All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are
#permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice, this list of
#      conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright notice, this list
#      of conditions and the following disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY Do@ ``AS IS'' AND ANY EXPRESS OR IMPLIED
#WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#The views and conclusions contained in the software and documentation are those of the
#authors and should not be interpreted as representing official policies, either expressed
#or implied, of Do@.

#Importer for locations from http://www.maxmind.com databases


import csv
import logging
import redis

from importer import Importer
from iprange import IPRange
import os

class GeoCityLiteImporter(Importer):


    def __init__(self, fileName , redisHost, redisPort, redisDB, locationFileName=None, regionsFileName=None, countriesFileName=None):
        Importer.__init__(self, fileName, redisHost, redisPort, redisDB)
        if not locationFileName:
            dirname = os.path.dirname(fileName)
            locationFileName = os.path.join(dirname, 'GeoLiteCity-Location.csv')
        if not regionsFileName:
            dirname = os.path.dirname(fileName)
            regionsFileName = os.path.join(dirname, 'regions.csv')
        if not countriesFileName:
            dirname = os.path.dirname(fileName)
            countriesFileName = os.path.join(dirname, 'countries.csv')
        self.locationFileName = locationFileName
        self.regionsFileName = regionsFileName
        self.countriesFileName = countriesFileName

    def buildCountryTable(self):
        """
        File Format:
        Country Name,ISO 3166-2 Code,ISO 3166-3 Code,Number
        """
        print "Building country table..."

        try:
            fp = open(self.countriesFileName)
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.countriesFileName, e))
            return False

        reader = csv.DictReader(fp, fieldnames=['countryName', 'countryCode', 'countryCode3', 'number'], delimiter=',', quotechar='"')
        country_table = {}
        for row in reader:
            row['countryName'] = row['countryName'].title()
            country_table[row['countryCode']] = row
        self.countryTable = country_table

    def buildRegionsTable(self):
        """
        File Format:
        Country Code,Region Code,Region Name
        """
        print "Building region table..."

        try:
            fp = open(self.regionsFileName)
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.regionsFileName, e))
            return False

        reader = csv.reader(fp, delimiter=',', quotechar='"')
        region_table = {}
        for row in reader:
            country_code, region_code, region_name = row
            region_sub_table = region_table.get(country_code, {})
            region_sub_table[region_code] = region_name
            region_table[country_code] = region_sub_table
        self.regionTable = region_table


    def buildLocationsTable(self):
        """
        File Format:
        "LocId","Country(ISO)","Region(FIPS)","City","postCode","lat","lon","metra","areaCode"
        "UNITED STATES","CALIFORNIA","LOS ANGELES","34.045200","-118.284000","90001"
        """
        print "Building location table..."

        try:
            fp = open(self.locationFileName)
            fp.readline()   # Skip the copyright line
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.locationFileName, e))
            return False

        reader = csv.DictReader(fp, delimiter=',', quotechar='"')
        location_table = {}
        for row in reader:
            country = row['country']
            region = row['region']
            if country in self.regionTable and region in self.regionTable[country]:
                row['regionName'] = self.regionTable[country][region]
            else:
                row['regionName'] = region

            if country in self.countryTable:
                row['countryName'] = self.countryTable[country]['countryName']
                row['countryCode'] = country
                row['countryCode3'] = self.countryTable[country]['countryCode3']
            else:
                row['countryName'] = country

            location_table[row['locId']] = row
        self.locationTable = location_table

    def runImport(self, reset=False):
        """
        File Format:
        Regions
        Country Code,Region Code,Region Name
        
        Block File
        "startNum","endNum","LocId"        
        """
        if reset:
            print "Deleting old ip data..."
            self.redis.delete(IPRange._indexKey)

        self.buildCountryTable()
        self.buildRegionsTable()
        self.buildLocationsTable()

        print "Starting import..."

        try:
            fp = open(self.fileName)
            fp.readline()   # Skip the copyright line
            fp.readline()   # Skip the header line
        except Exception, e:
            logging.error("could not open file %s for reading: %s" % (self.fileName, e))
            return False

        reader = csv.reader(fp, delimiter=',', quotechar='"')
        pipe = self.redis.pipeline()

        i = 0
        for row in reader:

            try:
                #parse the row
                locId = row[2]
                location = self.locationTable[locId]
                countryCode = location['country']
                rangeMin = int(row[0])
                rangeMax = int(row[1])
                lat = float(location['latitude'])
                lon = float(location['longitude'])

                #take the zipcode if possible
                try:
                    zipcode = location['postalCode']
                except:
                    zipcode = ''

                #junk record
                if countryCode == '-' and (not lat and not lon):
                    continue

                rng = IPRange(rangeMin, rangeMax, lat, lon, zipcode)
                rng.save(pipe)

            except Exception, e:
                logging.error("Could not save record: %s" % e)

            i += 1
            if i % 10000 == 0:
                logging.info("Dumping pipe. did %d ranges" % i)
                pipe.execute()

        pipe.execute()
        logging.info("Imported %d locations" % i)

        return i


