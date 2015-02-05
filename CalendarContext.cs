using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Entity;

namespace googleCalendar.Models
{
    public class CalendarEvent 
     
    {
       public List<googleCalendar.Models.gEvent> myEvent {get;set;} //Init the evevt 
       public CalendarEvent()
           
        {
          
             myEvent = new List<googleCalendar.Models.gEvent>()
            {
                new googleCalendar.Models.gEvent(){Id = 1,StartDate = new DateTime(2015, 2, 2, 9, 30, 30),EndDate = new DateTime(2015, 2, 2, 14, 30, 30),Description = "meeting1"},
                new googleCalendar.Models.gEvent(){Id = 2,StartDate = new DateTime(2015, 2, 3, 9, 30, 30),EndDate = new DateTime(2015, 2, 3, 15, 30, 30),Description = "meeting2"},
                new googleCalendar.Models.gEvent(){Id = 3,StartDate = new DateTime(2015, 2, 4, 9, 30, 30),EndDate = new DateTime(2015, 2, 4, 15, 30, 30),Description = "meeting3"},
            };
       }
    }
}

