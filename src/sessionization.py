import os
import datetime
from collections import OrderedDict

# Files necessary to test the program. Ensure that directory path names are correct and relative paths are accurate.

# timeoutfile_path = os.path.normpath("../input/inactivity_period.txt")
# weblogfile_path = os.path.normpath("../input/log.csv")
# outputfile_path = os.path.normpath("../output/sessionization.txt")

timeoutfile_path = os.path.normpath("../insight_testsuite/tests/my_test04/input/inactivity_period.txt")
weblogfile_path = os.path.normpath("../insight_testsuite/tests/my_test04/input/log.csv")
outputfile_path = os.path.normpath("../insight_testsuite/tests/my_test04/output/sessionization.txt")

col_delimiter = ','  # Column Delimiter in input and output files
datetime_fmt = '%Y-%m-%d %H:%M:%S'  # Datetime format

# Two dictionaries will be used to process the data from input file.
# We have need to effectively lookup against ip addresses as well as against datetime

ip_dict = {}
datetime_dict = OrderedDict()

curr_date = None
curr_time = None
curr_datetime = None
last_date = None
last_time = None
last_datetime = None


# Funtion to return difference in seconds between two string datetime values provided in datatime_fmt

def getSessDuration(begtime, endtime):
    endtime_obj = datetime.datetime.strptime(endtime, datetime_fmt)
    begtime_obj = datetime.datetime.strptime(begtime, datetime_fmt)
    sess_duration = endtime_obj - begtime_obj
    # Note that beg / end times are inclusive so need to add 1 to the difference
    return int(sess_duration.total_seconds()) + 1


# Funtion to record the session details to output file

def recordSessionInfo(outf, ipaddr, iprec):
    sess_lastime = iprec[2] + ' ' + iprec[3]
    sess_begtime = iprec[0] + ' ' + iprec[1]
    sess_duration = getSessDuration(sess_begtime, sess_lastime)
    sess_requests = iprec[4]

    # Construct the output string
    outstr = ipaddr + col_delimiter + sess_begtime + col_delimiter + \
             sess_lastime + col_delimiter + str(sess_duration) + col_delimiter \
             + str(sess_requests) + '\n'
    outf.write(outstr)

    return None


# Get the duration to define inactive session. This is expected to be in seconds from 1 to 86400.

with open(timeoutfile_path, 'r') as ftimeout:
    line = ftimeout.readline()
    inactiv_secs = datetime.timedelta(seconds=int(line))

# Begin Processing the Weblog.

with open(weblogfile_path, 'r') as fweblog:
    # Process the header line. Get the column locations for columns of interest.
    # This step potentially can be eliminated if order of columns is confirmed to be consistent.

    header_line = fweblog.readline()
    header_line = header_line.rstrip('\n')

    colnames = header_line.split(col_delimiter)  # Convert header line to a list

    # Locate the position for each column of interest, assumed col headings are consistent.

    ipPos = colnames.index("ip")
    datePos = colnames.index("date")
    timePos = colnames.index("time")
    cikPos = colnames.index("cik")
    accessionPos = colnames.index("accession")
    extentionPos = colnames.index("extention")

    # Open up the output file for writing.

    with open(outputfile_path, 'w') as foutfile:

        # Process the actual log entries starting at line 2

        for log_line in fweblog:

            # Get the column values from each line

            log_line = log_line.rstrip('\n')
            colvalues = log_line.split(col_delimiter)

            # Checking that all elements identifing the access to a file are not null in the weblog.
            # We will ignore these columns in further processing as valid log entry indicated a webpage request.

            if not colvalues[cikPos] or not colvalues[accessionPos] or not colvalues[extentionPos]:
                continue

            # Values of interest from the weblog

            curr_ip = colvalues[ipPos]
            curr_date = colvalues[datePos]
            curr_time = colvalues[timePos]
            curr_datetime = curr_date + ' ' + curr_time

            # For the first weblog record in the file, we just need to check this.

            if last_datetime is None:
                last_datetime = curr_datetime

            # If the timestamp of current record in the weblog file is higher than last record processed,
            # we need to check for past session completions provided inactivity period is past

            if curr_datetime > last_datetime:

                # Calculate the timestamp cutoff for inactive sessions.
                # We will use this against the datetime_dict for potential completed session ip addresses.
                # All sessions with last_datetime less than cuttoff_datetime will be marked complete.

                curr_datetime_obj = datetime.datetime.strptime(curr_datetime, datetime_fmt)
                cutoff_datetime_obj = curr_datetime_obj - inactiv_secs
                cutoff_datetime = cutoff_datetime_obj.strftime(datetime_fmt)

                # Get list of all potential completed (inactive) session ip addresses from datetime_dict.
                # From the ordered dict datatime_dict for each timestamp that is older than cutoff datetime
                # get all potential completed sessions .. in unique list using set. Purge these dict entries.

                inactiv_sess = set()
                remove_keys = []

                for key, value in datetime_dict.items():
                    if key >= cutoff_datetime:
                        break

                    for each_ip in value:
                        inactiv_sess.add(each_ip)

                    remove_keys.append(key)

                # Purge datetime dict records past the inactivity cutoff

                for key in remove_keys:
                    datetime_dict.pop(key)

                # Process now the possible session completions

                remove_sess = []
                for each_ip in inactiv_sess:
                    sess_lastime = ip_dict[each_ip][2] + ' ' + ip_dict[each_ip][3]

                    # Session is only completed if its last recorded access time is past cutoff

                    if sess_lastime < cutoff_datetime:
                        remove_sess.append(each_ip)
                        recordSessionInfo(foutfile, each_ip, ip_dict[each_ip])

                # Removing old ip dict records with completed sessions

                for key in remove_sess:
                    ip_dict.pop(key)

            last_datetime = curr_datetime

            # ip dictionary will have ip address as key and value will be a list of attributes necessary to
            # understand session and requests.
            # value list for the session will in this order: beg_date, beg_time, last_date, last_time, cumul_requests.
            # when new ip session is identified, we will use curr_date/time for beg/last_date/time.

            if curr_ip in ip_dict:
                ip_dict[curr_ip][2] = curr_date
                ip_dict[curr_ip][3] = curr_time
                ip_dict[curr_ip][4] += 1
            else:
                ip_dict[curr_ip] = [curr_date, curr_time, curr_date, curr_time, 1]

            # datetime dictionary will have all unique log entry timestamps as keys.
            # the values for each timestamp will be all ip addresses recorded at that timestamp

            if curr_datetime not in datetime_dict:
                datetime_dict[curr_datetime] = set()

            datetime_dict[curr_datetime].add(curr_ip)

        # Now that all lines in the weblog have been processed, mark all sessions completed in output file
        for key, value in ip_dict.items():
            recordSessionInfo(foutfile, key, value)

# ip_dict.clear()
# datetime_dict.clear()
print("Complete")
