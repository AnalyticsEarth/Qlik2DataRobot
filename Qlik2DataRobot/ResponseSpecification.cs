using System;
using System.Collections.Generic;
using System.Text;

namespace Qlik2DataRobot
{
    class ResponseSpecification
    {
        public List<DataSpecification> data { get; set; }

        public MetaResponseSpecification response { get; set; }

        public string message { get; set; }
    }

    class MetaResponseSpecification
    {
        public string id { get; set; }
    }

    class DataSpecification
    {
        public List<PredictionValueSpecification> predictionValues { get; set; }
        public double predictionThreshold { get; set; }
        public dynamic prediction { get; set; }
        public int rowId { get; set; }

        //Passthrough
        public Dictionary<string,dynamic> passthroughValues { get; set; }

        //Timeseries
        public dynamic seriesId { get; set; }
        public string forecastPoint { get; set; }
        public string timestamp { get; set; }
        public int forecastDistance { get; set; }
        public string originalFormatTimestamp { get; set; }

        //Explanations
        public List<PredictionExplanationSpecification> predictionExplanations { get; set; }
    }

    class PredictionValueSpecification
    {
        public string label { get; set; }
        public double value { get; set; }
    }

    class PredictionExplanationSpecification
    {
        public dynamic featureValue { get; set; }
        public double strength { get; set; }
        public string feature { get; set; }
        public string qualitativeStrength { get; set; }
        public dynamic label { get; set; } 
    }
}
