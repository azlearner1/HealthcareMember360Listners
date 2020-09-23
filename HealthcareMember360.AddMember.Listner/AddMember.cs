using System;
using System.Data.SqlClient;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace HealthcareMember360.AddMember.Listner
{
    public static class AddMember
    {
        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("addmember-topic", "addmember-subscription", Connection = "AzureBusConnectionString")]string mySbMsg, ILogger log)
        {
            log.LogInformation($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
            var member = JsonConvert.DeserializeObject<Member>(mySbMsg);
            if (member != null)
            {
                var str = Environment.GetEnvironmentVariable("SqlConnectionString");
                Random rd = new Random();
                var physicianId = rd.Next(1, 8);
                log.LogInformation($"Physician Id : {physicianId}");
                using (SqlConnection con = new SqlConnection(str))
                {
                    con.Open();
                    var query = "Insert into Member Values('" + member.FirstName + "','" + member.LastName + "','" + member.Address + "','" + member.State + "','" + member.EmailAddress + "','" + member.SSN + "'," + physicianId + ")";
                    log.LogInformation($"insert query : {query}");
                    using (SqlCommand cmd = new SqlCommand(query, con))
                    {
                        var rows = cmd.ExecuteNonQueryAsync();
                        log.LogInformation($"{rows} rows were updated");
                    }
                    con.Close();
                }
            }

        }
    }
    public class Member
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Address { get; set; }
        public string State { get; set; }
        public string EmailAddress { get; set; }
        public string SSN { get; set; }
    }
}
