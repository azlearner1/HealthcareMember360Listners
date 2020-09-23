using System;
using System.Data.SqlClient;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace HealthcareMember360.UpdateMember.Listner
{
    public static class UpdateMember
    {
        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("updatemember-topic", "updatemember-sub", Connection = "AzureBusConnectionString")]string mySbMsg, ILogger log)
        {
            log.LogInformation($"C# ServiceBus topic trigger function processed message: {mySbMsg}");

            var member = JsonConvert.DeserializeObject<Member>(mySbMsg);

            if (member != null)
            {
                var str = Environment.GetEnvironmentVariable("SqlConnectionString");
                using (SqlConnection con = new SqlConnection(str))
                {
                    con.Open();
                    var query = "update Member set FirstName = '" + member.FirstName + "', LastName = '" + member.LastName + "', Address = '"+ member.Address +"', State = '"+ member.State +"', EmailAddress = '"+ member.EmailAddress +"', SSN = '"+ member.SSN +"', PhysicianId = "+ member.PhysicianId + " where MemberID = " + member.MemberID;
                    log.LogInformation($"Update query : {query}");
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
        public int MemberID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Address { get; set; }
        public string State { get; set; }
        public string EmailAddress { get; set; }
        public string SSN { get; set; }
        public int PhysicianId { get; set; }
    }
}
