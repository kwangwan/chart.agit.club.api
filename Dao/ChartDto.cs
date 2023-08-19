namespace chart.agit.club.api.Dao
{
    public class TwitchChatBuzzInput
    {
        public string? ChartId { get; set; }
        public string? ChannelName { get; set; }
        public string? DateTimeStart { get; set; }
        public string? DateTimeEnd { get; set; }
        public string? TimeZone { get; set; }
        public string? CalendarInterval { get; set; }
        public List<string>? ShouldMatchPhrase { get; set; }
        public List<string>? MustMatchPhrase { get; set; }
        public List<string>? MustNotMatchPhrase { get; set; }

    }

    public class TwitchChatBuzzOutput
    {
        public string? ChartId { get; set; }
        public Dictionary<string, long?>? Value { get; set; }
    }

    public class TwitchChatELKFormat
    {
        public string? channel { get; set; }
        public string? chat { get; set; }
        public DateTime? datetime { get; set; }
    }
}
