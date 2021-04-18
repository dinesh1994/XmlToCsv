import logging
import csv
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import requests
import re
from zipfile import ZipFile
from optparse import OptionParser


logging.basicConfig(filename="app.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

'''
This Classs Takes Base xml File and does following...
-Parse and extract downloadble links
-Download, extract and save file from links
-For all saved file generate csv from xml with provided columns and conditions
'''
class XmlToCsv():
	def __init__(self,logger, xmlFile, ndownloads=1 ):
		self.xmlFile = xmlFile

		#Initializing list of Downloadable items
		self.downloadables = []
		self.Ndownloads = ndownloads
		self.logger=logger
		self.save_dir = 'data'
		self.saved_xml_files = []
		self.columns = ['FinInstrmGnlAttrbts.Id','FinInstrmGnlAttrbts.FullNm','FinInstrmGnlAttrbts.ClssfctnTp','FinInstrmGnlAttrbts.CmmdtyDerivInd','FinInstrmGnlAttrbts.NtnlCcy','Issr']


	def parseXML(self):
		self.logger.info("Parsing base xml")

		# create element tree object
		tree = ET.parse(self.xmlFile)
		root = tree.getroot()
		

		for item in root.findall('./result/doc'):
		    downloadable = False
		    temp_download_link = None
		    for child in item.findall('./str'):
		        if child.attrib.get('name')=='file_type' and child.text=='DLTINS':
		            downloadable = True
		        if child.attrib.get('name')=='download_link':
		            temp_download_link = child.text
		    if downloadable:
		        self.downloadables.append(temp_download_link)
		self.logger.info("downloadables link list updated")

	'''
	Download And Extract zip file to save_path location
	'''
	def downloadExtractNSave(self):
		total_links = len(self.downloadables)

		self.logger.info("Downloading {} out of total {} links".format(self.Ndownloads, total_links))
		for i in range(self.Ndownloads):
			if i>total_links:
				self.logger.info("Cannot proceed as total links available are {}".format(total_links))
				break
			try:
				url = self.downloadables[i]
				base_name = "{}".format(url.rsplit('/',1)[-1].split('.')[0])
				save_path = '{}/{}.zip'.format(self.save_dir,base_name)

				r = requests.get(url, allow_redirects=True)
				with open(save_path, 'wb') as f:
					f.write(r.content) 
				logger.info("File save to {}".format(save_path))

				with ZipFile(save_path,'r') as zip:
				    # extracting all the files
				    self.logger.info('Extracting all the files now...')
				    zip.extractall('data')
				    self.logger.info('Done!')
			except Exception as e:
				self.logger.error(str(e))
				pass
			else:
				self.saved_xml_files.append("{}/{}.xml".format(self.save_dir, base_name))

	def getFinInstrmGnlAttrbts(self,node):
		temp_dict = {col: None for col in self.columns}

		for child in node:
		    key = None
		    if 'Id' in child.tag:
		        key = 'FinInstrmGnlAttrbts.Id'
		    if 'FullNm' in child.tag:
		        key = 'FinInstrmGnlAttrbts.FullNm'
		    if 'ClssfctnTp' in child.tag:
		        key = 'FinInstrmGnlAttrbts.ClssfctnTp'
		    if 'CmmdtyDerivInd' in child.tag:
		        key = 'FinInstrmGnlAttrbts.CmmdtyDerivInd'
		    if 'NtnlCcy' in child.tag:
		        key = 'FinInstrmGnlAttrbts.NtnlCcy'
		    if key:
		        temp_dict[key] = child.text
		return temp_dict

	'''
	Extract the required tag to dictionary and return array of such dictionary 
	'''
	def getValueViaXpath(self, root, array_items=[]):
		for node in root.findall("./*/*/*/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrm/{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}TermntdRcrd"):
			temp_dict = {col: None for col in self.columns}
			for child in node.findall("./"):
				if 'FinInstrmGnlAttrbts' in child.tag:
					temp_dict.update(self.getFinInstrmGnlAttrbts(child))
				if 'Issr' in child.tag:
					temp_dict['Issr'] = child.text
			array_items.append(temp_dict)

		return array_items

	'''
	Function parse saved xml files and generate csv file with same name in same directory
	'''
	def xmlToCsv(self):
		self.logger.info("Starting conversion from xml to csv")
		for xmlFile in self.saved_xml_files:
			try:
				xmlparse = ET.parse(xmlFile)
				root = xmlparse.getroot()
				df = pd.DataFrame(self.getValueViaXpath(root))
				csv_file_name = "{}/{}.csv".format(self.save_dir, xmlFile.rsplit('/',1)[-1].split('.')[0])
				df.to_csv(csv_file_name, sep=',')
			except Exception as e:
				self.logger.error(str(e))
				pass
			else:
				self.logger.info("csv file {} generated".format(csv_file_name))


def main(base_xml, ndownloads):
	logger.info("Initialzing {} file parsing".format(base_xml))
	xmlObj = XmlToCsv(logger, base_xml, ndownloads)
	xmlObj.parseXML()
	xmlObj.downloadExtractNSave()
	xmlObj.xmlToCsv()

if __name__=='__main__':
	parser = OptionParser()
	parser.add_option('-i','--input-xml',type=str,help="Base xml file containing downloadable links")
	parser.add_option('-d','--number-of-downloads',type=int,default=1,help="Total downloads to be made from Base xml file containing downloadable links")
	(options, args) = parser.parse_args()
	main(options.input_xml, options.number_of_downloads)
