
import React, { useRef } from 'react';

interface FileUploadZoneProps {
  onFilesSelected: (files: File[]) => void;
  label: string;
  accept?: string;
  isFolder?: boolean;
}

const FileUploadZone: React.FC<FileUploadZoneProps> = ({ onFilesSelected, label, accept, isFolder }) => {
  const inputRef = useRef<HTMLInputElement>(null);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      onFilesSelected(Array.from(e.target.files));
      // Reset value so the same selection can be triggered again if needed
      e.target.value = '';
    }
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      onFilesSelected(Array.from(e.dataTransfer.files));
    }
  };

  const handleClick = () => {
    inputRef.current?.click();
  };

  return (
    <div 
      onDragOver={handleDragOver}
      onDrop={handleDrop}
      onClick={handleClick}
      className="border-2 border-dashed border-slate-300 rounded-xl p-8 text-center hover:border-indigo-500 hover:bg-indigo-50 transition-all cursor-pointer group relative overflow-hidden"
    >
      <input 
        type="file" 
        className="hidden" 
        ref={inputRef}
        onChange={handleChange}
        accept={accept}
        multiple
        {...(isFolder ? { webkitdirectory: "", directory: "" } as any : {})}
      />
      <div className="flex flex-col items-center gap-3 relative z-10">
        <div className="p-3 bg-indigo-100 rounded-full text-indigo-600 group-hover:scale-110 transition-transform">
          {isFolder ? (
            <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
            </svg>
          ) : (
            <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
          )}
        </div>
        <div>
          <p className="text-slate-600 font-bold">{label}</p>
          <p className="text-slate-400 text-sm mt-1">
            {isFolder ? "Select a folder or drag multiple images" : "Select files or drag and drop"}
          </p>
        </div>
      </div>
      
      {/* Decorative background element */}
      <div className="absolute inset-0 bg-indigo-50/0 group-hover:bg-indigo-50/40 transition-colors pointer-events-none" />
    </div>
  );
};

export default FileUploadZone;
