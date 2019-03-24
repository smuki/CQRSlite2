using System;
using System.Collections.Generic;
using System.Text;
//using CQRSCode.WriteModel.Commands;
using CQRSlite.Commands;
using System.Threading;
using System.Threading.Tasks;
//using CQRSCode.ReadModel.Queries;
using CQRSlite.Queries;
using CQRSCode.WriteModel.Commands;

namespace ConsoleApp
{
    public class HomeController 
    {
        private readonly ICommandSender _commandSender;
        private readonly IQueryProcessor _queryProcessor;

        public HomeController(ICommandSender commandSender, IQueryProcessor queryProcessor)
        {
            _commandSender = commandSender;
            _queryProcessor = queryProcessor;
        }

        //public async Task<ActionResult> Index()
        //{
        //    ViewData.Model = await _queryProcessor.Query(new GetInventoryItems());

        //    return View();
        //}

        //public async Task<ActionResult> Details(Guid id)
        //{
        //    ViewData.Model = await _queryProcessor.Query(new GetInventoryItemDetails(id));
        //    return View();
        //}

        //public ActionResult Add()
        //{
        //    return View();
        //}


        public async Task Add(string name, CancellationToken cancellationToken)
        {
            await _commandSender.Send(new CreateInventoryItem(Guid.NewGuid(), name), cancellationToken);
        }

        //public async Task<ActionResult> ChangeName(Guid id)
        //{
        //    ViewData.Model = await _queryProcessor.Query(new GetInventoryItemDetails(id));
        //    return View();
        //}

        //[HttpPost]
        public async Task ChangeName(Guid id, string name, int version, CancellationToken cancellationToken)
        {
            await _commandSender.Send(new RenameInventoryItem(id, name, version), cancellationToken);
        }

        public async Task Deactivate(Guid id, int version, CancellationToken cancellationToken)
        {
             await _commandSender.Send(new DeactivateInventoryItem(id, version), cancellationToken);
        }

        //public async Task<ActionResult> CheckIn(Guid id)
        //{
        //    ViewData.Model = await _queryProcessor.Query(new GetInventoryItemDetails(id));
        //    return View();
        //}

        public async Task CheckIn(Guid id, int number, int version, CancellationToken cancellationToken)
        {
            await _commandSender.Send(new CheckInItemsToInventory(id, number, version), cancellationToken);
        }

        //public async Task<ActionResult> Remove(Guid id)
        //{
        //    ViewData.Model = await _queryProcessor.Query(new GetInventoryItemDetails(id));
        //    return View();
        //}

        public async Task Remove(Guid id, int number, int version, CancellationToken cancellationToken)
        {
            await _commandSender.Send(new RemoveItemsFromInventory(id, number, version), cancellationToken);
        }
    }
}
