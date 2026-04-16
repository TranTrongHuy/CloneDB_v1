using Microsoft.Extensions.Logging;
using Microsoft.Maui.LifecycleEvents;

#if WINDOWS
using Microsoft.UI;
using Microsoft.UI.Windowing;
using Windows.Graphics;
using static Microsoft.IO.RecyclableMemoryStreamManager;
#endif

namespace CloneDB
{
    public static class MauiProgram
    {
        public static MauiApp CreateMauiApp()
        {
            var builder = MauiApp.CreateBuilder();
            builder
                .UseMauiApp<App>()
                .ConfigureFonts(fonts =>
                {
                    fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
                });

            builder.Services.AddMauiBlazorWebView();
            builder.Services.AddSingleton<CloneService>();
            builder.ConfigureLifecycleEvents(events =>
            {
#if WINDOWS
                events.AddWindows(wndLifeCycleBuilder =>
                {
                    wndLifeCycleBuilder.OnWindowCreated(window =>
                    {
                        IntPtr nativeWindowHandle = WinRT.Interop.WindowNative.GetWindowHandle(window);
                        WindowId nativeWindowId = Win32Interop.GetWindowIdFromWindow(nativeWindowHandle);
                        AppWindow appWindow = AppWindow.GetFromWindowId(nativeWindowId);
                        var p = appWindow.Presenter as OverlappedPresenter;
                        window.ExtendsContentIntoTitleBar = false;
                        //p.SetBorderAndTitleBar(false, false);
                        p.Maximize();
                    });

                });
#endif
            });

#if DEBUG
            builder.Services.AddBlazorWebViewDeveloperTools();
    		builder.Logging.AddDebug();
#endif

            return builder.Build();
        }
    }
}
