using chart.agit.club.api.Dao;

namespace chart.agit.club.api.Data
{
    public interface ChartInterface
    {
        TwitchChatBuzzOutput GetTwitchChatBuzz(TwitchChatInput twitchChatInput);
        List<TwitchChatMessagesOutput> GetTwitchChatMessages(TwitchChatInput twitchChatInput);
    }
}
