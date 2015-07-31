SELECT f.pid, f.missing, f.storage, f.type, f.metadata,
	f.identifier, f.identifier_hash, f.folder_hash,
	f.extension, f.mime_type, f.name, f.sha1,
	f.size, m.width, m.height
FROM sys_file f
JOIN sys_file_metadata m ON f.uid = m.file
