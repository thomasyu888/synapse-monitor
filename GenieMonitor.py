#!/usr/bin/env python

import synapseclient
import calendar
import time
import argparse
import multiprocessing.dummy as mp
import pandas as pd
from synapseclient import Table


ONEDAY=86400000 #default delta t is 10 days prior

pd.options.mode.chained_assignment = None

def findNewFiles(args, id):
    """Performs query query to find changed entities in id. """

    QUERY = "select id, name, center, fileType, versionNumber, modifiedOn, modifiedByPrincipalId, nodeType from entity where projectId=='%s' and modifiedOn>%i" 
    t = calendar.timegm(time.gmtime())*1000
    project = syn.get(id)
    #Determine the last audit time or overide with lastTime
    if args.days is None:  #No time specified
        args.days = project.get('lastAuditTimeStamp', None)
        if args.days is None:  #No time specified and no lastAuditTimeStamp set
            args.days = t - ONEDAY*1.1
        else: #args.days came from annotation strip out from list
            args.days = args.days[0]  
    print t, args.days, id, (t-args.days)/float(ONEDAY), 'days'
    results = list(syn.chunkedQuery(QUERY % (id, args.days)))
    #Add the project and other metadata
    for r in results:
        r['projectId'] = id
        r['projectName'] = project.name
        r['date'] = synapseclient.utils.from_unix_epoch_time(r['entity.modifiedOn']).strftime("%b/%d/%Y %H:%M")
        r['user'] = syn.getUserProfile(r['entity.modifiedByPrincipalId'])['userName']
        r['type'] = r['entity.nodeType']
        
    #Set lastAuditTimeStamp
    if args.updateProject:
        project.lastAuditTimeStamp = t
        try:
            project = syn.store(project)
        except synapseclient.exceptions.SynapseHTTPError:
            pass
    return results

def composeMessage(entityList):
    """Composes a message with the contents of entityList """
    
    messageHead=('<h4>Time of Audit: %s </h4>'%time.ctime() +
                 '<table border=1><tr>'
                 '<th>Project</th>'
                 '<th>Entity</th>'
                 '<th>Ver.</th>'
                 '<th>Type</th>'
                 '<th>Change Time</th>'
                 '<th>Contributor</th></tr>'  )
    lines = [('<tr><td><a href="https://www.synapse.org/#!Synapse:%(projectId)s">%(projectName)s</a></td>'
              '<td><a href="https://www.synapse.org/#!Synapse:%(entity.id)s">(%(entity.id)s)</a> %(entity.name)s </td>'
              '<td>%(entity.versionNumber)s</td>'
              '<td>%(type)s</td>'
              '<td>%(date)s</td>'
              '<td><a href="https://www.synapse.org/#!Profile:%(entity.modifiedByPrincipalId)s">%(user)s</a></td></tr>')%item for 
             item in entityList]
    return messageHead + '\n'.join(lines)+'</table></body>'


def build_parser():
    """Set up argument parser and returns"""
    parser = argparse.ArgumentParser(
        description='Checks for new/modified entities in a project.')
    parser.add_argument('--userId', dest='userId',
                        help='User Id of individual to send report, defaults to current user.')
    parser.add_argument('--projects', '-p', metavar='projects', type=str, nargs='*',
            help='Synapse IDs of projects to be monitored.')
    parser.add_argument('--days', '-d', metavar='days', type=float, default=None,
            help='Find modifications in the last days')
    parser.add_argument('--updateProject', dest='updateProject',  action='store_true',
            help='If set will modify the annotations by setting lastAuditTimeStamp to the current time on each project.')
    parser.add_argument('--emailSubject', dest='emailSubject',  default = 'Updated Synapse Files',
            help='Sets the subject heading of the email sent out (defaults to Updated Synapse Files')
    parser.add_argument('--config', metavar='file', dest='configPath',  type=str,
            help='Synapse config file with user credentials (overides default ~/.synapseConfig)')
    return parser


p = mp.Pool(6)
args = build_parser().parse_args()
args.days = None if args.days is None else calendar.timegm(time.gmtime())*1000 - args.days*ONEDAY
if args.configPath is not None:
    syn=synapseclient.Synapse(skip_checks=True, configPath=args.configPath)
else:
    syn=synapseclient.Synapse(skip_checks=True)
syn.login(silent=True) 
args.userId = syn.getUserProfile()['ownerId'] if args.userId is None else args.userId


#query each project then combine into long list
entityList = p.map(lambda project: findNewFiles(args, project), args.projects)
entityList = [item for sublist in entityList for item in sublist]
#Filter out projects and folders
entityList = [e for e in entityList if e['entity.nodeType'] not in ['project', 'folder']]
print 'Total number of entities = ', len(entityList)

results = pd.DataFrame(entityList)
center = results['entity.center']
fileType = results['entity.fileType']

center[center.isnull()] = ''
fileType[fileType.isnull()] = ''

for index,i in enumerate(center):
    if i!='':
        results['entity.center'][index] = i[0]
for index,i in enumerate(fileType):
    if i!='':
        results['entity.fileType'][index] = i[0]

#results.to_csv("dummy.csv")
resultDf = pd.DataFrame(columns = ["entityName","changeTime","entityId","contributor","center","fileType"])
resultDf['entityName'] = results['entity.name']
resultDf['changeTime'] = results['entity.modifiedOn']
resultDf['entityId'] = results['entity.id']
resultDf['contributor'] = results['user']
resultDf['center'] = results['entity.center']
resultDf['fileType'] = results['entity.fileType']

week_interval = ONEDAY * 7

third = args.days + week_interval + 1
second = third + week_interval

#3rd week
firstdate = synapseclient.utils.from_unix_epoch_time(args.days).strftime("%b/%d/%Y")
#2nd week
secondstartdate = synapseclient.utils.from_unix_epoch_time(third).strftime("%b/%d/%Y")
secondenddate = synapseclient.utils.from_unix_epoch_time(third - ONEDAY).strftime("%b/%d/%Y")
#1st week
thirdstartdate = synapseclient.utils.from_unix_epoch_time(second).strftime("%b/%d/%Y")
thirdenddate = synapseclient.utils.from_unix_epoch_time(second - ONEDAY).strftime("%b/%d/%Y")
present = synapseclient.utils.from_unix_epoch_time(calendar.timegm(time.gmtime())*1000).strftime("%b/%d/%Y")

schema = syn.get("syn5874214")
existingtable = syn.tableQuery("select * from syn5874214")
existingtable = existingtable.asDataFrame()
emptyDate = [str(i) for i in resultDf['changeTime']]
newTable = resultDf['entityName'] + emptyDate
tableDate = [str(i) for i in existingtable['changeTime']]
oldTable = existingtable['entityName'] + tableDate

upload = resultDf[~newTable.isin(oldTable)]

print("Uploading new table entries")
syn.store(Table(schema, upload))

print("Updating wiki page")
wikipage = syn.getWiki("syn3380222",subpageId=396117)

markdown = ("#### _GENIE updates will be released here periodically_\n\n",
            "###%s to %s\n" % (thirdstartdate, present),
            "**Contributors**\n",
            "${synapsetable?query=SELECT center%2Ccontributor%2C COUNT%28%2A%29 FROM syn5874214 where ", 
            "changeTime > %d" % second,
            "GROUP BY contributor ORDER BY COUNT%28%2A%29 DESC&limit=5 }\n",
            "**fileTypes**\n",
            "${synapsetable?query=SELECT center%2CfileType%2C COUNT%28%2A%29 FROM syn5874214 where ", 
            "changeTime > %d" % second,
            "GROUP BY fileType ORDER BY COUNT%28%2A%29 DESC&limit=5 }\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT entityName%2CentityId%2Ccontributor%2Ccenter%2CfileType FROM syn5874214 where ",
            "changeTime > %d ORDER BY changeTime DESC&limit=5}\n\n" % second,
            "###%s to %s\n" % (secondstartdate,thirdenddate),
            "**Contributors**\n",
            "${synapsetable?query=SELECT center%2Ccontributor%2C COUNT%28%2A%29 FROM syn5874214 where ",
            "changeTime > %d and changeTime < %d" % (third,second),
            "GROUP BY contributor ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "**fileTypes**\n",
            "${synapsetable?query=SELECT center%2CfileType%2C COUNT%28%2A%29 FROM syn5874214 where ", 
            "changeTime > %d and changeTime < %d" % (third,second),
            "GROUP BY fileType ORDER BY COUNT%28%2A%29 DESC&limit=5 }\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT entityName%2CentityId%2Ccontributor%2Ccenter%2CfileType FROM syn5874214 where ",
            "changeTime > %d and changeTime < %d ORDER BY changeTime DESC&limit=5}\n\n" %(third, second),
            "###%s to %s\n" % (firstdate, secondenddate),
            "**Contributors**\n",
            "${synapsetable?query=SELECT center%2Ccontributor%2C COUNT%28%2A%29 FROM syn5874214 where ",
            "changeTime < %d" % third,
            "GROUP BY contributor ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "**fileTypes**\n",
            "${synapsetable?query=SELECT center%2CfileType%2C COUNT%28%2A%29 FROM syn5874214 where ", 
            "changeTime < %d" % third,
            "GROUP BY fileType ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT entityName%2CentityId%2Ccontributor%2Ccenter%2CfileType FROM syn5874214 where ",
            "changeTime < %d ORDER BY changeTime DESC&limit=5}" % third)
markdown = ''.join(markdown)
wikipage.markdown = markdown
syn.store(wikipage)


#Prepare and send Message
syn.sendMessage([args.userId], 
                args.emailSubject, 
                composeMessage(entityList),
                contentType = 'text/html')



