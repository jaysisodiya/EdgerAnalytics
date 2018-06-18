# EdgerAnalytics
Insight Data Science Coding Challenge

Submission by Jay Sisodiya (jaysisodiya@gmail.com / 925-984-9194)

- No external dependencies
- Tested with million records and performed under few seconds

The key challenge in processing these weblogs for our goal of computing
session completions is need to accessing the data via ip-addr as well as
via time-of-access. I have attepmted to achieve this using two ordered
dictionaries.

IP addr dict records ipaddr, session begin time, last access time, incremental webreq cnt
DateTime dict records for each distinct datetime in weblog list of all ip addresses

As we scan through the weblog and identify entry which has higher timestamp than
previous one, we go ahead and assess which session are now completed and can be recorded
to the output file. To achieve this, we scan the (ordered) datetime dict and stop after
cutoff datetime (curr record datetime - inactivity period) .. so we optimize and ensure
only necessary dict records are accessed. From here we get list of all unique ip
addresses, check now against the IP addr ordered dict to see if last access time for
session also falls behind the cutoff time .. if yes, we go ahead and record that
session completed.

At the end of file, we close out all sessions as completed.
