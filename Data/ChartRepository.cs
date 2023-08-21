using chart.agit.club.api.Dao;
using Nest;

namespace chart.agit.club.api.Data
{

    public class ChartRepository : ChartInterface
    {
        private readonly IConfiguration _config;

        public ChartRepository(IConfiguration config)
        {
            _config = config;
        }

        private ElasticClient GetElasticClient(string index)
        {
            ConnectionSettings Settings = new ConnectionSettings(new Uri(_config.GetSection("ElasticSearch_8:Uri").Value)).DefaultIndex(index).RequestTimeout(TimeSpan.FromMinutes(3));
            return new ElasticClient(Settings);
        }

        private SearchDescriptor<TwitchChatELKFormat> GetTwitchChartDefaultDescriptor(TwitchChatInput twitchChatBuzzInput)
        {
            string ChannelName = twitchChatBuzzInput.ChannelName ?? "";
            string DateTimeStart = twitchChatBuzzInput.DateTimeStart ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm");
            string DateTimeEnd = twitchChatBuzzInput.DateTimeEnd ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm");
            string TimeZone = twitchChatBuzzInput.TimeZone ?? "+00:00";
            string ShouldMatch = twitchChatBuzzInput.ShouldMatch ?? "";
            string MustMatch = twitchChatBuzzInput.MustMatch ?? "";
            string MustNotMatch = twitchChatBuzzInput.MustNotMatch ?? "";
            int MinimumShouldMatch = ShouldMatch.Length > 0 ? 1 : 0;

            // Query Containers
            List<QueryContainer> FilterQueryContainer = new List<QueryContainer>();
            List<QueryContainer> ShouldQueryContainer = new List<QueryContainer>();
            List<QueryContainer> MustQueryContainer = new List<QueryContainer>();
            List<QueryContainer> MustNotQueryContainer = new List<QueryContainer>();

            // date range
            QueryContainerDescriptor<TwitchChatELKFormat> DateRangeDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
            DateRangeDescriptor.DateRange(d => d
                .Field(field => field.datetime)
                .GreaterThanOrEquals(DateTimeStart)
                .LessThan(DateTimeEnd)
                .Format("yyyy-MM-dd HH:mm")
                .TimeZone(TimeZone)
            );
            FilterQueryContainer.Add(DateRangeDescriptor);

            // channel
            QueryContainerDescriptor<TwitchChatELKFormat> TermDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
            TermDescriptor.Term(t => t
                .Field(field => field.channel)
                .Value(ChannelName)
            );
            FilterQueryContainer.Add(TermDescriptor);

            // match
            QueryContainerDescriptor<TwitchChatELKFormat> ShouldMatchDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
            ShouldMatchDescriptor.Match(m => m.Field(field => field.chat).Query(ShouldMatch));
            ShouldQueryContainer.Add(ShouldMatchDescriptor);

            QueryContainerDescriptor<TwitchChatELKFormat> MustMatchDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
            MustMatchDescriptor.Match(m => m.Field(field => field.chat).Query(MustMatch));
            MustQueryContainer.Add(MustMatchDescriptor);

            QueryContainerDescriptor<TwitchChatELKFormat> MustNotMatchDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
            MustNotMatchDescriptor.Match(m => m.Field(field => field.chat).Query(MustNotMatch));
            MustNotQueryContainer.Add(MustNotMatchDescriptor);

            return new SearchDescriptor<TwitchChatELKFormat>()
                .Query(q => q
                    .Bool(b => b
                        .Filter(FilterQueryContainer.ToArray())
                        .Should(ShouldQueryContainer.ToArray())
                        .Must(MustQueryContainer.ToArray())
                        .Must(MustNotQueryContainer.ToArray())
                        .MinimumShouldMatch(MinimumShouldMatch)
                    )
                )
                .TrackTotalHits(true);

        }

        private SearchDescriptor<TwitchChatELKFormat> GetTwitchChartBuzzDescriptor(TwitchChatInput twitchChatInput)
        {
            Time FixedInterval = new Time(twitchChatInput.FixedInterval);
            string TimeZone = twitchChatInput.TimeZone ?? "+00:00";

            SearchDescriptor<TwitchChatELKFormat> TwitchChartDefaultDescriptor = GetTwitchChartDefaultDescriptor(twitchChatInput);
            return TwitchChartDefaultDescriptor
                .Size(0)
                .Aggregations(a => a
                    .DateHistogram("_datetime", d => d
                        .Field(field => field.datetime)
                        .FixedInterval(FixedInterval)
                        .Format("yyyy-MM-dd HH:mm")
                        .TimeZone(TimeZone)
                    )
                );
        }

        private SearchDescriptor<TwitchChatELKFormat> GetTwitchChartMessagesDescriptor(TwitchChatInput twitchChatInput)
        {
            Time FixedInterval = new Time(twitchChatInput.FixedInterval);
            string TimeZone = twitchChatInput.TimeZone ?? "+00:00";

            SearchDescriptor<TwitchChatELKFormat> TwitchChartDefaultDescriptor = GetTwitchChartDefaultDescriptor(twitchChatInput);
            return TwitchChartDefaultDescriptor.Size(1000);
        }

        private TwitchChatBuzzOutput ParseTwitchChatBuzzELKResponse(ISearchResponse<TwitchChatELKFormat> response, TwitchChatInput twitchChatBuzzInput)
        {
            TwitchChatBuzzOutput Result = new TwitchChatBuzzOutput();

            Result.ChartId = twitchChatBuzzInput.ChartId;
            Result.Value = new Dictionary<string, long?>();

            bool IsMinute = twitchChatBuzzInput.FixedInterval?.EndsWith("m") ?? false;
            bool IsHour = twitchChatBuzzInput.FixedInterval?.EndsWith("h") ?? false;
            bool IsDay = twitchChatBuzzInput.FixedInterval?.EndsWith("d") ?? false;
            bool IsNoInterval = !IsMinute && !IsHour && !IsDay; 

            int FixedIntervalLength = twitchChatBuzzInput.FixedInterval?.Length ?? 0;
            bool IsParsed = Int32.TryParse(twitchChatBuzzInput.FixedInterval?.Remove(FixedIntervalLength - 1, 1) ?? "0", out int FixedIntervalNum);
            if (!IsParsed)
            {
                FixedIntervalNum = 0;
                IsNoInterval = true;
            }

            DateTime DateTimeStart = DateTime.Parse(twitchChatBuzzInput.DateTimeStart ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
            DateTime DateTimeEnd = DateTime.Parse(twitchChatBuzzInput.DateTimeEnd ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
            if (IsMinute)
            {
                DateTimeStart = DateTimeStart.AddMinutes(-(DateTimeStart.Minute%FixedIntervalNum));
            }
            if (IsHour)
            {
                DateTimeStart = DateTimeStart.AddMinutes(-DateTimeStart.Minute);
                DateTimeStart = DateTimeStart.AddHours(-(DateTimeStart.Hour%FixedIntervalNum));
            }
            if (IsDay) 
            {
                DateTimeStart = DateTimeStart.AddMinutes(-DateTimeStart.Minute);
                DateTimeStart = DateTimeStart.AddHours(-DateTimeStart.Hour);
                DateTimeStart = DateTimeStart.AddDays(-(DateTimeStart.Day%FixedIntervalNum));
            }

            while (true)
            {
                string DateTimeStr = DateTimeStart.ToString("yyyy-MM-dd HH:mm");
                Result.Value.Add(DateTimeStr, 0);
                if (IsMinute) { DateTimeStart = DateTimeStart.AddMinutes(FixedIntervalNum); }
                if (IsHour) { DateTimeStart = DateTimeStart.AddHours(FixedIntervalNum); }
                if (IsDay) { DateTimeStart = DateTimeStart.AddDays(FixedIntervalNum); }
                if (IsNoInterval) { break; }
                if (DateTimeStart >= DateTimeEnd) { break; }
            }

            List<DateHistogramBucket> DateHistogramItems = response.Aggregations.DateHistogram("_datetime")?.Buckets.ToList() ?? new List<DateHistogramBucket>();

            foreach (var item in DateHistogramItems)
            {
                Result.Value[item.KeyAsString] = item.DocCount;
            };

            return Result;
        }

        private List<TwitchChatMessagesOutput> ParseTwitchChatMessageELKResponse(ISearchResponse<TwitchChatELKFormat> response)
        {
            List<TwitchChatMessagesOutput> Result = new List<TwitchChatMessagesOutput>();

            List<IHit<TwitchChatELKFormat>> HitItems = response.Hits.OrderBy(x => x.Source.datetime).ToList();
            foreach (var item in HitItems)
            {
                string CreatedAt = item.Source.datetime?.ToString("yyyy-MM-dd") ?? "";
                string Chat = item.Source.chat ?? "";
                Result.Add(new TwitchChatMessagesOutput { CreatedAt = CreatedAt, Chat = Chat });
            }

            return Result;
        }

        public TwitchChatBuzzOutput GetTwitchChatBuzz(TwitchChatInput twitchChatInput)
        {
            ElasticClient Client = GetElasticClient("twitch-chat");
            SearchDescriptor<TwitchChatELKFormat> Descriptor = GetTwitchChartBuzzDescriptor(twitchChatInput);

            // test
            var stream = new System.IO.MemoryStream();
            Client.RequestResponseSerializer.Serialize(Descriptor, stream);
            var jsonQuery = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            Console.WriteLine(jsonQuery);

            ISearchResponse<TwitchChatELKFormat> Response = Client.Search<TwitchChatELKFormat>(Descriptor);

            TwitchChatBuzzOutput Result = ParseTwitchChatBuzzELKResponse(Response, twitchChatInput);

            return Result;
        }

        public List<TwitchChatMessagesOutput> GetTwitchChatMessages(TwitchChatInput twitchChatInput)
        {
            ElasticClient Client = GetElasticClient("twitch-chat");
            SearchDescriptor<TwitchChatELKFormat> Descriptor = GetTwitchChartMessagesDescriptor(twitchChatInput);

            // test
            var stream = new System.IO.MemoryStream();
            Client.RequestResponseSerializer.Serialize(Descriptor, stream);
            var jsonQuery = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            Console.WriteLine(jsonQuery);

            ISearchResponse<TwitchChatELKFormat> Response = Client.Search<TwitchChatELKFormat>(Descriptor);

            List<TwitchChatMessagesOutput> Result = ParseTwitchChatMessageELKResponse(Response);

            return Result;
        }
    }
}
