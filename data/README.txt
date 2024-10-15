source: https://www.kaggle.com/datasets/olxdatascience/olx-jobs-interactions?resource=download


SUMMARY
================================================================================
This dataset (olx-jobs) is published by Grupa OLX sp. z o.o. and contains 65 502 201 events made on http://olx.pl/praca by 3 295 942 users who interacted with 185 395 job ads in 2 weeks of 2020.


INTERACTIONS FILE DESCRIPTION
================================================================================
The file interactions.csv consists of 65 502 201 rows. 
Each row represents an interaction between a user and an item and has the following format:
user, item, event, timestamp.

* user: a numeric id representing the user who made the interaction
* item: a numeric id representing the item the user interacted with
* event: a type of interaction between the user and the item, possible values are:
	- click: the user visited the item detail page
	- bookmark: the user added the item to bookmarks
	- chat_click: the user opened the chat to contact the item’s owner
	- contact_phone_click_1: the user revealed the phone number attached to the item
	- contact_phone_click_2: the user clicked to make a phone call to the item’s owner
	- contact_phone_click_3: the user clicked to send an SMS to the item’s owner
	- contact_partner_click: the user clicked to access the item’s owner external page
	- contact_chat: the user sent a message to the item’s owner
* timestamp: the Unix timestamp of the interaction

Maintaining the confidentiality of ads and users was a priority when preparing this dataset. The measures taken to protect privacy included the following:
* original user and item identifiers were replaced by unique random integers,
* some undisclosed constant integer was added to all timestamps,
* some fraction of interactions were filtered out,
* some additional artificial interactions were added.


LICENSE
================================================================================
The data we publish may be re-used under the following conditions:
* The data may be re-used solely as a base or supplement for conducting an analysis or research, incl. academic analysis or research, results of which may be further published; nevertheless under no circumstances the data set itself may be further published in parts or as a whole;
* The data may be re-used solely for non-commercial purposes;
* The user may not state or imply that the analysis or research conducted based on the data is in any way endorsed, supported, financed or commissioned by Grupa OLX sp. z o.o. or other entities or brands belonging to the same capital group as Grupa OLX sp. z o.o. although the publication of the results of given analysis or research must acknowledge that they have been produced, solely or partially, in connection with the use of the data set made available by Grupa OLX sp. z o.o.;
* The re-use of the data, incl. the publication of the results of given analysis or research, may not infringe the reputation or good name of Grupa OLX sp. z o.o. or other entities or brands belonging to the same capital group as Grupa OLX sp. z o.o.;
* The copy of the given publication and its source (or at least specific indication of the source, if the copy cannot be freely shared by the user) must be shared  with Grupa OLX sp. z o.o. via e-mail address indicated in CONTACT DETAILS section below as soon as the publication is confirmed;
* The permission to re-use the data within described scope applies worldwide, is royalty-free, non-exclusive, cannot be transferred to another party and can be revoked by Grupa OLX sp. z o.o. at any time;
* Except as expressly stated, this permission does not grant the user of the data any rights to, or in, patents, copyrights, database rights, trade names, trademarks (whether registered or unregistered and whether future or existing), or any other rights or licences in respect of the intellectual property owned by Grupa OLX sp. z o.o. and  other entities belonging to the same capital group as Grupa OLX sp. z o.o.


WARRANTIES AND LIABILITY LIMITATION
================================================================================
Grupa OLX sp. z o.o. assumes no responsibility for the accuracy, adequacy, validity and quality of the published data (nor for the accuracy, adequacy, validity and quality of the results of the given analysis or research produced in connection with the use of the data) as well as for its suitability for any particular purpose.


CONTACT DETAILS
================================================================================
Any questions and information about publications related to this dataset should be sent to olx-data-science-datasets@olx.com