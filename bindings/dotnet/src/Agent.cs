namespace Roam.DotNet;

using System.Runtime.InteropServices;

public class Agent
{
    public string Id { get; }

    public Agent(string id)
    {
        Id = id;
    }

    public void Connect()
    {
        // Call Rust FFI
        var ptr = NativeMethods.oam_agent_connect(Id);
        try
        {
            var result = Marshal.PtrToStringAnsi(ptr);
            Console.WriteLine(result);
        }
        finally
        {
            NativeMethods.oam_free_string(ptr);
        }
    }
}
