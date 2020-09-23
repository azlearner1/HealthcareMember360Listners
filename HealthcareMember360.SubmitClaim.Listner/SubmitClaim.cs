using System;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace HealthcareMember360.SubmitClaim.Listner
{
    public static class SubmitClaim
    {
        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("submitclaims", "submitclaims-sub", Connection = "AzureBusConnectionString")]string mySbMsg, ILogger log)
        {
            log.LogInformation($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
            var claimDetails = JsonConvert.DeserializeObject<ClaimDetails>(mySbMsg);
            if (claimDetails != null)
            {
                var str = Environment.GetEnvironmentVariable("SqlConnectionString");
                using (SqlConnection con = new SqlConnection(str))
                {
                    DataTable dtClaim = new DataTable();
                    DataTable dtMember = new DataTable();
                    int ClaimTypeID = 0, MemberID = 0;
                    
                    var getClaimTypeQuery = "select ClaimTypeID from ClaimTypes where ClaimType = '" + claimDetails.ClaimType + "'"; 
                    var getMemberIDQuery = "select MemberID from Member where Upper(FirstName) + ' ' + Upper(LastName) = Upper('" + claimDetails.MemberName + "')";
                    using (SqlCommand cmd = new SqlCommand(getClaimTypeQuery, con))
                    {
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dtClaim);
                            if(dtClaim.Rows.Count > 0)
                            {
                                ClaimTypeID = Convert.ToInt32(dtClaim.Rows[0]["ClaimTypeID"]);
                            }
                        }
                    }

                    using (SqlCommand cmd = new SqlCommand(getMemberIDQuery, con))
                    {
                        using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                        {
                            da.Fill(dtMember);
                            if (dtMember.Rows.Count > 0)
                            {
                                MemberID = Convert.ToInt32(dtMember.Rows[0]["MemberID"]);
                            }
                        }
                    }
                    con.Open();
                    var query = "Insert into Claims Values (" + ClaimTypeID + "," + claimDetails.ClaimAmount + ",'" + claimDetails.ClaimDate.ToString("MM/dd/yyyy") + "','" + claimDetails.Remarks + "'," + MemberID + ")";
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

    public class ClaimDetails
    {
        public string ClaimType { get; set; }
        public int ClaimAmount { get; set; }
        public DateTime ClaimDate { get; set; }
        public string Remarks { get; set; }
        public string MemberName { get; set; }
    }
}
