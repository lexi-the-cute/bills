import json

# https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/BillEndpoint.md#item-level
class Bill:
	def __init__(self, congress, billType, billNumber):
		self.congress = congress
		self.billType = billType
		self.billNumber = billNumber
	
	def setCongress(self, congress):
		self.congress = congress
		
	def setBillType(self, billType):
		self.billType = billType.lower()
		
	def setBillNumber(self, billNumber):
		self.billNumber = billNumber
	
	def setOriginChamber(self, originChamber):
		self.originChamber = originChamber
		
	def setIntroducedDate(self, introducedDate):
		self.introducedDate = introducedDate
	
	def setUpdateDate(self, updateDate):
		self.updateDate = updateDate
		
	def setUpdateDateIncludingText(self, updateDateIncludingText):
		self.updateDateIncludingText = updateDateIncludingText
	
	# This one only exists for billType "hr" or "hjres"
	def setConstitutionalAuthorityStatementText(self, constitutionalAuthorityStatementText):
		self.constitutionalAuthorityStatementText = constitutionalAuthorityStatementText
	
	def setTitle(self, title):
		self.title = title
	
	# Starting Below Are The One's With Children
	# TODO: Determine How To Handle These Children
	def setCommittees(self, count, url):
		self.committeesCount = count
		self.committeesURL = url
	
	# This one will be a special object
	def setCommitteeReports(self, committeeReportList):
		self.committeeReportList = committeeReportList
	
	def setRelatedBills(self, count, url):
		self.relatedBillsCount = count
		self.relatedBillsURL = url

	def setActions(self, count, url):
		self.actionsCount = count
		self.actionsURL = url

	# This one will be a special object
	def setSponsors(self, sponsorList):
		self.sponsorList = sponsorList

	def setCosponsors(self, countIncludingWithdrawn, count, url):
		self.cosponsorsCountIncludingWithdrawn = countIncludingWithdrawn
		self.cosponsorsCount = count
		self.cosponsorsURL = url

	# This one will be a special object
	def setCBOCostEstimates(self, cboCostEstimateList):
		self.cboCostEstimateList = cboCostEstimateList

	# The pluralization is intentional
	def setLaws(self, lawType, lawNumber):
		self.lawType = lawType
		self.lawNumber = lawNumber

	# This one will be a special object
	def setNotes(self, notesList):
		self.notesList = notesList

	def setPolicyArea(self, name):
		self.policyAreaName = name

	def setSubjects(self, count, url):
		self.subjectsCount = count
		self.subjectsURL = url

	def setSummaries(self, count, url):
		self.summariesCount = count
		self.summariesURL = url

	def setTitles(self, count, url):
		self.titlesCount = count
		self.titlesURL = url

	def setAmendments(self, count, url):
		self.amendmentsCount = count
		self.amendmentsURL = url
		
	def setTextVersions(self, count, url):
		self.textVersionsCount = count
		self.textVersionsURL = url

	def setLatestAction(self, date, time, text):
		self.latestActionDate = date
		self.latestActionTime = time
		self.latestActionText = text

	def __str__(self):
		return f"<Bill:{self.congress}-{self.billType}-{self.billNumber}>"

class CommitteeReportStub:
	def __init__(self, citation, url):
		self.citation = citation
		self.url = url
	
	def __str__(self):
		return f"<CommitteeReportStub:{self.citation}>"

class Sponsor:
	def __init__(self, bioguideId, fullName, party):
		self.bioguideId = bioguideId
		self.fullName = fullName
		self.party = party
	
	def setBioguideId(self, bioguideId):
		self.bioguideId = bioguideId
	
	def setFullName(self, fullName):
		self.fullName = fullName
	
	def setParty(self, party):
		self.party = party
	
	def setFirstName(self, firstName):
		self.firstName = firstName
		
	def setMiddleName(self, middleName):
		self.middleName = middleName
		
	def setLastName(self, lastName):
		self.lastName = lastName
	
	def setState(self, state):
		self.state = state
		
	def setURL(self, url):
		self.url = url
	
	def setDistrict(self, district):
		self.district = district
	
	def setSponsoredByRequest(self, request: bool):
		self.sponsoredByRequest = request
	
	def __str__(self):
		return f"<Sponsor:{self.fullName}>"

class Note:
	def __init__(self, text):
		self.text = text
	
	def __str__(self):
		return f"<Note:{self.text}>"

def createBill(data: dict):
	item = data["bill"]

	bill = Bill(item["congress"], item["type"], item["number"])
	
	if "constitutionalAuthorityStatementText" in item:
		bill.setConstitutionalAuthorityStatementText(item["constitutionalAuthorityStatementText"])
	
	if "introducedDate" in item:
		bill.setIntroducedDate(item["introducedDate"])
	
	if "latestAction" in item:
		time = item["latestAction"]["actionTime"] if "actionTime" in item["latestAction"] else None  # Ternary Operator
		bill.setLatestAction(item["latestAction"]["actionDate"], time, item["latestAction"]["text"])
	
	if "originChamber" in item:
		bill.setOriginChamber(item["originChamber"])

	if "title" in item:
		bill.setTitle(item["title"])

	if "updateDate" in item:
		bill.setUpdateDate(item["updateDate"])
		
	if "updateDateIncludingText" in item:
		bill.setUpdateDateIncludingText(item["updateDateIncludingText"])

	#setCommitteeReports(committeeReportList)
	#setCBOCostEstimates(cboCostEstimateList)
	#setLaws(lawType, lawNumber)
	#setNotes(notesList)
	#setPolicyArea(name)

	# TODO: Deal With URL Stubs
	if "actions" in item:
		bill.setActions(item["actions"]["count"], item["actions"]["url"])
			
	if "committees" in item:
		bill.setCommittees(item["committees"]["count"], item["committees"]["url"])

	if "titles" in item:
		bill.setTitles(item["titles"]["count"], item["titles"]["url"])

	if "sponsors" in item:
		sponsors: list = []
		for sponsorData in item["sponsors"]:
			sponsor = Sponsor(sponsorData["bioguideId"], sponsorData["fullName"], sponsorData["party"])
			sponsor.setDistrict(sponsorData["district"])
			sponsor.setFirstName(sponsorData["firstName"])
			sponsor.setLastName(sponsorData["lastName"])
			sponsor.setSponsoredByRequest(sponsorData["isByRequest"])
			sponsor.setState(sponsorData["state"])
			sponsor.setURL(sponsorData["url"])
			
			if "middleName" in sponsorData:
				sponsor.setMiddleName(sponsorData["middleName"])
			
			sponsors.append(sponsor)
			
		bill.setSponsors(sponsors)
	
	#setRelatedBills(count, url)
	#setCosponsors(countIncludingWithdrawn, count, url)
	#setSubjects(count, url)
	#setSummaries(count, url)
	#setAmendments(count, url)
	#setTextVersions(count, url)
	
	# TODO: End Deal With URL Stubs	
	
	return bill
	

if __name__ == "__main__":
	with open("local/usa/federal/congress/bills/118/hr/91/data.json", mode="r") as f:
		data = json.load(f)
		bill = createBill(data)

		print(bill)
