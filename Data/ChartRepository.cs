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

        private DateInterval GetCalendarInterval(string? calendarInterval)
        {
            bool IsMinute = calendarInterval == "minute";
            bool IsHour = calendarInterval == "hour";
            bool IsDay = calendarInterval == "day";
            bool IsWeek = calendarInterval == "week";
            bool IsMonth = calendarInterval == "month";
            bool IsQuarter = calendarInterval == "quarter";
            bool IsYear = calendarInterval == "year";

            if (IsMinute) { return DateInterval.Minute; }
            if (IsHour) { return DateInterval.Hour; }
            if (IsDay) { return DateInterval.Day; }
            if (IsWeek) { return DateInterval.Week; }
            if (IsMonth) { return DateInterval.Month; }
            if (IsQuarter) { return DateInterval.Quarter; }
            return DateInterval.Year;
        }

        private SearchDescriptor<TwitchChatELKFormat> GetTwitchChartDefaultDescriptor(TwitchChatBuzzInput twitchChatBuzzInput)
        {
            string ChannelName = twitchChatBuzzInput.ChannelName ?? "";
            string DateTimeStart = twitchChatBuzzInput.DateTimeStart ?? DateTime.Now.AddHours(-1).ToString("yyyy-MM-dd HH:mm");
            string DateTimeEnd = twitchChatBuzzInput.DateTimeEnd ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm");
            string TimeZone = twitchChatBuzzInput.TimeZone ?? "+00:00";
            List<string> ShouldMatchPhrase = twitchChatBuzzInput.ShouldMatchPhrase ?? new List<string>();
            List<string> MustMatchPhrase = twitchChatBuzzInput.MustMatchPhrase ?? new List<string>();
            List<string> MustNotMatchPhrase = twitchChatBuzzInput.MustNotMatchPhrase ?? new List<string>();

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
                .LessThanOrEquals(DateTimeEnd)
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

            // match_phrase
            ShouldMatchPhrase.ForEach(query =>
            {
                QueryContainerDescriptor<TwitchChatELKFormat> MatchPhraseDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
                MatchPhraseDescriptor.MatchPhrase(m => m.Field(field => field.chat).Query(query).Slop(1));
                ShouldQueryContainer.Add(MatchPhraseDescriptor);
            });
            MustMatchPhrase.ForEach(query =>
            {
                QueryContainerDescriptor<TwitchChatELKFormat> MatchPhraseDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
                MatchPhraseDescriptor.MatchPhrase(m => m.Field(field => field.chat).Query(query).Slop(1));
                MustQueryContainer.Add(MatchPhraseDescriptor);
            });
            MustNotMatchPhrase.ForEach(query =>
            {
                QueryContainerDescriptor<TwitchChatELKFormat> MatchPhraseDescriptor = new QueryContainerDescriptor<TwitchChatELKFormat>();
                MatchPhraseDescriptor.MatchPhrase(m => m.Field(field => field.chat).Query(query).Slop(1));
                MustNotQueryContainer.Add(MatchPhraseDescriptor);
            });

            return new SearchDescriptor<TwitchChatELKFormat>()
                .Query(q => q
                    .Bool(b => b
                        .Filter(FilterQueryContainer.ToArray())
                        .Should(ShouldQueryContainer.ToArray())
                        .Must(MustQueryContainer.ToArray())
                        .Must(MustNotQueryContainer.ToArray())
                        .MinimumShouldMatch(1)
                    )
                )
                .TrackTotalHits(true);

        }

        private SearchDescriptor<TwitchChatELKFormat> GetTwitchChartBuzzDescriptor(TwitchChatBuzzInput twitchChatBuzzInput)
        {
            DateInterval CalendarInterval = GetCalendarInterval(twitchChatBuzzInput.CalendarInterval);
            string TimeZone = twitchChatBuzzInput.TimeZone ?? "+00:00";

            SearchDescriptor<TwitchChatELKFormat> TwitchChartDefaultDescriptor = GetTwitchChartDefaultDescriptor(twitchChatBuzzInput);
            return TwitchChartDefaultDescriptor
                .Size(0)
                .Aggregations(a => a
                    .DateHistogram("_datetime", d => d
                        .Field(field => field.datetime)
                        .CalendarInterval(CalendarInterval)
                        .Format("yyyy-MM-dd HH:mm")
                        .TimeZone(TimeZone)
                    )
                );
        }

        private TwitchChatBuzzOutput ParseTwitchChatELKResponse(ISearchResponse<TwitchChatELKFormat> response, string? charId)
        {
            TwitchChatBuzzOutput Result = new TwitchChatBuzzOutput();

            Result.ChartId = charId;
            Result.Value = new Dictionary<string, long?>();

            foreach (var item in response.Aggregations.DateHistogram("_datetime").Buckets.ToList())
            {
                Result.Value.Add(item.KeyAsString, item.DocCount);
            };
            
            

            // 추가

            return Result;
        }

        public TwitchChatBuzzOutput GetTwitchChatBuzz(TwitchChatBuzzInput twitchChatBuzzInput)
        {
            ISearchResponse<TwitchChatELKFormat> Response = GetElasticClient("twitch_chat")
                .Search<TwitchChatELKFormat>(GetTwitchChartBuzzDescriptor(twitchChatBuzzInput));

            TwitchChatBuzzOutput Result = ParseTwitchChatELKResponse(Response, twitchChatBuzzInput.ChartId);

            return Result;
        }
    }
}
