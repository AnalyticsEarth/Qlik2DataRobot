using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Qlik2DataRobot
{
    class RequestSpecification
    {
        public string request_type { get; set; }
        public AuthConfigSpecification auth_config { get; set; }
        public string mlops_endpoint { get; set; }
        public string dataset_name { get; set; }
        public string project_name { get; set; }
        public string deployment_id { get; set; }

        public string dataset_id { get; set; }
        public string project_id { get; set; } //Backwards Compatible (not in published spec)
        public string model_id { get; set; } //Backwards Compatible (not in published spec)

        public string keyfield { get; set; }

        [DefaultValue(false)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public bool should_cache { get; set; }

        [DefaultValue(false)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public bool inc_details { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public ExplainSpecification explain { get; set; }

        public string forecast_point { get; set; }
        public string timestamp_field { get; set; }
        
        [DefaultValue("s")]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public string timestamp_format { get; set; }

        public string association_id_name { get; set; }
        public string target_name { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string passthroughColumnsSet { get; set; }
    }

    class AuthConfigSpecification
    {
        public string api_token { get; set; }
        public string endpoint { get; set; }
        public string datarobot_key { get; set; }
    }

    class ExplainSpecification
    {
        [DefaultValue(3)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public int max_codes { get; set; }

        [DefaultValue(0.5)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public double threshold_high { get; set; }

        [DefaultValue(0.15)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public double threshold_low { get; set; }

        [DefaultValue(true)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public bool return_raw { get; set; }
    }
}
