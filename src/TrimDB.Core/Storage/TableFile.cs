using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrimDB.Core.Storage
{
    public class TableFile
    {
        private string _fileName;
        private const int PageSize = 4096;
        private const uint MagicNumber = 0xDEADBEAF;

        public TableFile(string fileLocation, int level, int fileId)
        {
            _fileName = System.IO.Path.Combine(fileLocation, $"Level{level}_{fileId}.trim");
        }

        internal ValueTask<(SearchResult result, Memory<byte> value)> GetAsync(ReadOnlyMemory<byte> key, ulong hash)
        {
            throw new NotImplementedException();
        }

        public async Task Load()
        {
            using var fs = System.IO.File.OpenRead(_fileName);
            var length = fs.Length;
            fs.Seek(-PageSize, System.IO.SeekOrigin.End);

            var pageBuffer = new byte[PageSize];

            var totalRead = 0;


            while (totalRead < PageSize)
            {
                var result = await fs.ReadAsync(pageBuffer, totalRead, pageBuffer.Length - totalRead);
                if (result == -1)
                {
                    throw new InvalidOperationException("Could not read any data from the file");
                }
                totalRead += result;
            }
            ReadTOC(pageBuffer);
        }

        private void ReadTOC(ReadOnlySpan<byte> buffer)
        {

        }
    }
}
