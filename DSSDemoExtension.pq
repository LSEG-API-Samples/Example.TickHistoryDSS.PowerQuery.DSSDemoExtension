// This file contains your Data Connector logic
[Version = "1.0.0"]
section DSSDemoExtension;

// Helper Fuction
Value.WaitFor = (producer as function, interval as function, optional count as number) as any =>
    let
        list = List.Generate(
            () => {0, null},
            (state) => state{0} <> null and (count = null or state{0} < count),
            (state) => if state{1} <> null then {null, state{1}} else {1 + state{0}, Function.InvokeAfter(() => producer(state{0}), interval(state{0}))},
            (state) => state{1})
    in
        List.Last(list);

// Request authentication token
Login = (user,pass,currenttime) =>
    let
        endpoint = "https://selectapi.datascope.lseg.com/RestApi/v1/Authentication/RequestToken",
        body = "{""Credentials"": {""Username"": """ & user & """,""Password"": """ & pass & """}}",
        response=Web.Contents(endpoint,[
                        Headers=[#"Content-Type"="application/json",
                                MyHeader=currenttime], 
                        Content=Text.ToBinary(body)]), 
        responseBody= Json.Document(response),
        value = responseBody[value]
    in
        "Token"&value;



CheckStatus = (token,location) =>
    let
        jobId = Value.WaitFor(
        (iteration) =>
            let
                url = location,
                response = Web.Contents(url, [Headers=[Authorization=token, #"Content-Type"="application/json"],ManualStatusHandling={202}]),
                status = Value.Metadata(response)[Response.Status],
                jobId=if status = 200 then Json.Document(response)[JobId]
                else null
            in
                jobId,
        (iteration) => #duration(0, 0, 0, Number.Power(2, iteration)),5)
        
    in
        jobId;

GetData = (token,jobId) =>
    let
        endpoint = "https://selectapi.datascope.lseg.com/RestApi/v1/Extractions/RawExtractionResults('" & jobId & "')/$value",
        Source = Csv.Document(Web.Contents(
                endpoint, 
                [Headers=
                    [Authorization=token, 
                    #"Content-Type"="application/json"]
                ]),
                [Delimiter=",", Encoding=1252, QuoteStyle=QuoteStyle.None])
    in
        Source;

ExtractRTH_TimeSeries = (token, ric) =>
    let
        endpoint = "https://selectapi.datascope.lseg.com/RestApi/v1/Extractions/ExtractRaw",
        body = 
        "{
            ""ExtractionRequest"": 
            {
                ""@odata.type"": ""#DataScope.Select.Api.Extractions.ExtractionRequests.ElektronTimeseriesExtractionRequest"",
                ""ContentFieldNames"": 
                [
                    ""Instrument ID"",
                    ""Bid"",
                    ""Ask"",
                    ""High"",
                    ""Low"",
                    ""Last"",
                    ""Volume"",
                    ""Trade Date""
                ],
                ""IdentifierList"": 
                {
                    ""@odata.type"": ""#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"",
                    ""InstrumentIdentifiers"":
                    [
                        {
                            ""Identifier"":"""& ric &""",
                            ""IdentifierType"":""Ric""
                        }
                    ]
                },
                ""Condition"":
                {
                    ""ReportDateRangeType"":""Relative"",
                    ""RelativeEndDaysAgo"": 1,
                    ""RelativeStartDaysAgo"": 30
                }
            }
        }",
        response = Web.Contents(
                        endpoint, 
                        [Headers=
                            [#"Authorization"=token, 
                             #"Content-Type"="application/json",
                             #"Prefer"= "respond-async;wait=1"], 
                         Content=Text.ToBinary(body),
                         ManualStatusHandling={400,202}]),
        status = Value.Metadata(response)[Response.Status],

        jobId=if status = 200 then Json.Document(response)[JobId]
        else if status = 202 then CheckStatus(token,Value.Metadata(response)[Headers][#"Location"])
        else Error.Record("HTTP Status",Text.From(status), Json.Document(response)[error][message])
    in
        jobId;

ExtractDSS_Intraday= (token, ric) =>
    let
        endpoint = "https://selectapi.datascope.lseg.com/RestApi/v1/Extractions/ExtractRaw",
        body = 
    "{
      ""ExtractionRequest"": 
          {
            ""@odata.type"": ""#DataScope.Select.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest"",
            ""ContentFieldNames"": [
                ""RIC"",
                ""Ask Price"",
                ""Bid Price"",
                ""Currency Code"",
                ""Exchange Code"",
                ""High Price"",
                ""Low Price"",
                ""Open Price"",
                ""Previous Close Date"",
                ""Previous Close Price"",
                ""Security Description"",
                ""Last Update Time""
            ],
            ""IdentifierList"": {
                ""@odata.type"": ""#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList"",  
                ""InstrumentIdentifiers"": [
                    { ""Identifier"": ""JPY="", ""IdentifierType"": ""Ric"" },
                    { ""Identifier"": ""EUR="", ""IdentifierType"": ""Ric"" },
                    { ""Identifier"": ""INR="", ""IdentifierType"": ""Ric"" }
                ]
            },
            ""Condition"": { ""ScalableCurrency"": true }
            }
        }",
        response = Web.Contents(
                        endpoint, 
                        [Headers=
                            [#"Authorization"=token, 
                             #"Content-Type"="application/json",
                             #"Prefer"= "respond-async"], 
                         Content=Text.ToBinary(body),
                         ManualStatusHandling={400,202},
                         IsRetry=true]),
        status = Value.Metadata(response)[Response.Status],

        jobId=if status = 200 then Json.Document(response)[JobId]
        else if status = 202 then CheckStatus(token,Value.Metadata(response)[Headers][#"Location"])
        else Error.Record("HTTP Status",Text.From(status), Json.Document(response)[error][message])
    in
        jobId;

[DataSource.Kind="DSSDemoExtension", Publish="DSSDemoExtension.Publish1"]
shared DSSDemoExtension.RTHTimeSeries = (Ricname as text) =>
    let 
    DSSUserName = Extension.CurrentCredential()[Username],
    DSSPassword = Extension.CurrentCredential()[Password],
    currentdate = Date.ToText(DateTime.Date(DateTime.LocalNow())),
    //1)	Request authentication token
    token = Login(DSSUserName,DSSPassword,currentdate),
    //2)	Request historical data from the server, using an on demand request and 3)	Check the request status
    jobId = ExtractRTH_TimeSeries(token,Ricname),
    //4)	Retrieve the data
    source = if Value.Is(jobId, type text) then
        GetData(token,jobId)
    else 
        jobId
    in
        source;

[DataSource.Kind="DSSDemoExtension", Publish="DSSDemoExtension.Publish2"]
shared DSSDemoExtension.DSSIntraday = (Ricname as text) =>
    let 
    DSSUserName = Extension.CurrentCredential()[Username],
    DSSPassword = Extension.CurrentCredential()[Password],
    currentdate = Date.ToText(DateTime.Date(DateTime.LocalNow())),
    token = Login(DSSUserName,DSSPassword,currentdate),
    jobId = ExtractDSS_Intraday(token,Ricname),
    source = if Value.Is(jobId, type text) then
        GetData(token,jobId)
    else 
        jobId
    in
        source;

// Data Source Kind description
DSSDemoExtension = [
    Authentication = [
        UsernamePassword = [UsernameLabel="DSS Username",PasswordLabel="DSS Password"]
    ],
    Label = Extension.LoadString("DataSourceLabel")
];


// Data Source UI publishing description
DSSDemoExtension.Publish1 = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle1"), Extension.LoadString("ButtonHelp1") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = DSSDemoExtension.Icons,
    SourceTypeImage = DSSDemoExtension.Icons
];

// Data Source UI publishing description
DSSDemoExtension.Publish2 = [
    Beta = true,
    Category = "Other",
    ButtonText = { Extension.LoadString("ButtonTitle2"), Extension.LoadString("ButtonHelp2") },
    LearnMoreUrl = "https://powerbi.microsoft.com/",
    SourceImage = DSSDemoExtension.Icons,
    SourceTypeImage = DSSDemoExtension.Icons
];

DSSDemoExtension.Icons = [
    Icon16 = { Extension.Contents("DSSDemoExtension16.png"), Extension.Contents("DSSDemoExtension20.png"), Extension.Contents("DSSDemoExtension24.png"), Extension.Contents("DSSDemoExtension32.png") },
    Icon32 = { Extension.Contents("DSSDemoExtension32.png"), Extension.Contents("DSSDemoExtension40.png"), Extension.Contents("DSSDemoExtension48.png"), Extension.Contents("DSSDemoExtension64.png") }
];

