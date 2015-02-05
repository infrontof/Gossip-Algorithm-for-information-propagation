using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Security;
using System.Data;
using System.Data.Entity;
using DHTMLX.Scheduler;
using DHTMLX.Scheduler.Data;
using DHTMLX.Common;
using googleCalendar.Models;


namespace googleCalendar.Controllers
{
    public class HomeController : Controller
    {
        private CalendarEvent existEvent = new CalendarEvent();
      
    
        public ActionResult Index()
        {
            var scheduler = new DHXScheduler(this);           
            scheduler.Skin = DHXScheduler.Skins.Terrace;
            scheduler.Config.first_hour = 0;
            scheduler.Config.last_hour = 24;
            scheduler.EnableDynamicLoading(SchedulerDataLoader.DynamicalLoadingMode.Month);            
            return View(scheduler);
        }
         public ActionResult login(LoginModel model)// login can also pull data from google Api
        {
            if (ModelState.IsValid)
            {
                if (model.Username == "ping ying" && model.Password == "123")
                {
                    FormsAuthentication.SetAuthCookie(model.Username, false);
                    return RedirectToAction("profile", "Home");
                }
                {
                    ModelState.AddModelError("", "Invalid username or password");
                }
            }
             return View();
        }

         public ActionResult profile()
         {
             var scheduler = new DHXScheduler(this);             
             scheduler.Skin = DHXScheduler.Skins.Terrace;
             scheduler.Config.first_hour = 0;
             scheduler.Config.last_hour = 24;
             scheduler.EnableDynamicLoading(SchedulerDataLoader.DynamicalLoadingMode.Month);
              scheduler.LoadData = true;
              scheduler.EnableDataprocessor = true;

             return View(scheduler);
         }

        public ContentResult Data(DateTime from, DateTime to)
        {

            var myiterator = existEvent.myEvent.Where(e => e.StartDate < to && e.EndDate >= from).ToList();          
            return new SchedulerAjaxData(myiterator);
        }

        public ActionResult Save(int? id, FormCollection actionValues)
        {
            var action = new DataAction(actionValues);

            try
            {
                var changedEvent = DHXEventsHelper.Bind<gEvent>(actionValues);
                switch (action.Type)
                {
                    case DataActionTypes.Insert:
                        existEvent.myEvent.Add(changedEvent);
                        break;
                    case DataActionTypes.Delete: //will add later                     
                        break;
                    default:// "update" add later                       
                        break;
                }
              
                
                action.TargetId = changedEvent.Id;
            }
            catch (Exception a)
            {
                action.Type = DataActionTypes.Error;
            }

            return (new AjaxSaveResponse(action));
        }
    }
}
