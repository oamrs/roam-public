using System.Runtime.InteropServices;

namespace Roam.DotNet;

internal static class NativeMethods
{
    private const string LibName = "oam";

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr oam_version();

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr oam_agent_connect(string agentId);

    [DllImport(LibName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void oam_free_string(IntPtr ptr);
}
