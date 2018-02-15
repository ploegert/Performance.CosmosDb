using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace loadteseting.function
{
    public class UserDocument
    {
        //"id": "int-1million",
        [JsonProperty("id")]
        public string Id { get; set; }

        //"objectType": "User",
        [JsonProperty("objectType")]
        public string ObjectType { get; set; }

        //"userId": "string-16",
        [JsonProperty("userId")]
        public string UserId { get; set; }

        //"userFirstName": "string-64",
        [JsonProperty("userFirstName")]
        public string UserFirstName { get; set; }

        //"userLastName": "string-64",
        [JsonProperty("userLastName")]
        public string UserLastName { get; set; }

        //"password": "string-8",
        [JsonProperty("password")]
        public string Password { get; set; }

        //"jwt": "100byte"
        [JsonProperty("jwt")]
        public string Jwt { get; set; }
        
    }
}
