﻿namespace chart.agit.club.api.Dao
{
    public class TwitchChatInput
    {
        public string? ChartId { get; set; }
        public string? ChannelName { get; set; }
        public string? DateTimeStart { get; set; }
        public string? DateTimeEnd { get; set; }
        public string? TimeZone { get; set; }
        public string? FixedInterval { get; set; }
        public string? ShouldMatch { get; set; }
        public string? MustMatch { get; set; }
        public string? MustNotMatch { get; set; }

    }

    public class TwitchChatBuzzOutput
    {
        public string? ChartId { get; set; }
        public Dictionary<string, long?>? Value { get; set; }
    }

    public class TwitchChatMessagesOutput
    {
        public string? CreatedAt { get; set; }
        public string? Chat { get; set; }
    }

    public class TwitchChatELKFormat
    {
        public string? channel { get; set; }
        public string? chat { get; set; }
        public DateTime? datetime { get; set; }
    }
}
