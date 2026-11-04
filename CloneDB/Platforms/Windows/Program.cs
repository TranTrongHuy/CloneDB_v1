namespace CloneDB.WinUI
{
    // Hàm Main thay cho bản tự sinh: bắt buộc chạy luồng giao diện ở chế độ STA thì WebView2 (BlazorWebView) mới khởi tạo được
    public static class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            WinRT.ComWrappersSupport.InitializeComWrappers();
            Microsoft.UI.Xaml.Application.Start(p =>
            {
                var context = new Microsoft.UI.Dispatching.DispatcherQueueSynchronizationContext(Microsoft.UI.Dispatching.DispatcherQueue.GetForCurrentThread());
                SynchronizationContext.SetSynchronizationContext(context);
                new App();
            });
        }
    }
}
