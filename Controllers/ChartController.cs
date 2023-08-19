using chart.agit.club.api.Dao;
using chart.agit.club.api.Data;
using Microsoft.AspNetCore.Mvc;

namespace chart.agit.club.api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ChartController : ControllerBase
    {
        private readonly ChartInterface _chart;

        public ChartController(ChartInterface chart)
        {
            _chart = chart;
        }

        [HttpPost("twitch_chat_buzz")]
        public TwitchChatBuzzOutput GetTwitchChatBuzz(TwitchChatBuzzInput twitchChatBuzzInput)
        {
            return _chart.GetTwitchChatBuzz(twitchChatBuzzInput);
        }
    }
}
