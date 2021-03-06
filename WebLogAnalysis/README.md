The solution has three main modules.

1. Data preparation. Selection,transformation and filtering.
2. Analyze time difference data between two consecutive request and try to come up with a optimized inactivity duration to identify session boundary.Core assumption in this
   analysis is that the user behavior is random,so by the statistics emperical rule the 2SD value for the interval would be a good choice to include 95% of cases.
3. Sessionize the data based on inactivity duration found in the previous step and answer following questions.

        Determine the average session time
        Determine unique URL visits per session
        Find the most engaged users, ie the IPs with the longest session times

Data Preparation:

    Unique user: IP alone is not a great identifier for an individual user, from initial analysis we found that IPs like 52.74.219.71 (Google BOT),
    220.226.206.7 (seems to be a service which is requesting recharge) were coming at the top for unique page visit during a session or longest session duration. Experimented with different
    inactivity interval like 15,30,60 etc. and the findings were consistent. Request associated with these IPs didn't had a user-agent associated with them, hence decided to extract the
    machine type and include that alongside the IP to identify a unique user. Also filtering out the records where machine type couldn't be determined.

    Unique URL: URLs under the "request" column are parsed and only the "PATH" component was extracted to identify unique URLs interacted during the session. Considering
    the full URL including query parameter would make most of them unique and won't effectively identify the resource requested.
    Further work can be done to improve this for e.g.
        REST API endpoints where an identifier like product id is passed to retrieve the details can be trimmed off
        the identifier to be recognized as once single unique resource.
        We can also filter requests which are coming for html resources like icons and images.

    IP Extraction: IP is extracted by parsing the "client-port" and only keeping the IP. (scope of improvement) We are not checking whether the IP is strictly following this pattern XXX.XXX.XXX.XXX,
    so an IP like 070.168.168.001 could be treated differently from 70.168.168.1

    Filter the group of IP+ client machine type where only one request found. Since we need at least two request to construct a session.
    Filter all records without a client device type from user agent.

Inactivity Interval Analysis:

    Considering user navigation behavior is a completely random activity and we have sufficiently large sample size then by empirical rule the 2 SD value can give us the time interval
    value between two consecutive request where 95% user will make another request.Performed a min-max normalization on the time interval column (which gives the difference in seconds between
    previous and current request) to minimize the impact of large outliers.Then calculated mean and population standard deviation. Took the 2SD value and transformed it back to seconds
    from normalized form.

Results:

    Inactivity duration for session boundary chosen 2765 seconds.

    Following IPs got removed due to filtering of missing user agent.
        220.226.206.7 = Seems to be a service which is requesting recharge. Doesn't look like an actual user.
        52.74.219.71 = Next most engaged user having long session length and most uniq url visits is a GOOGLE BOT.

    Avg session length in seconds:  705.8581210874822
    Avergae URL visits per session: 8.750932224457049

    Max session length in seconds: 7879
    Most engaged user :
        ip = 119.81.61.166
        client machine type = Macintosh; Intel Mac OS X 10_7_3







