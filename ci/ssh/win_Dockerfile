# syntax = docker/dockerfile:1.2.1
FROM mcr.microsoft.com/windows/servercore:ltsc2019
SHELL ["powershell"]

ENV PYTHON_VERSION 3.8.10
ENV PYTHON_RELEASE 3.8.10

# RUN --mount=type=bind,source=D:/swdvlp64_cmake/external/ipyparallel,target=C:/src/ipyparallel

#download and install latest node js version
RUN $baseurl = 'https://nodejs.org/dist/latest/'; \
	$line = Invoke-WebRequest -UseBasicParsing -Uri $baseurl | Select-Object -ExpandProperty Content | ForEach-Object { $_.Split(\"`r`n\") } | Select-String -Pattern '-x64.msi'; \
	$filename = ($line -split '>' -split '<')[2]; \
	Write-Host ('Latest node.js installer {0}' -f $filename); \
	$url = ('{0}{1}' -f $baseurl, $filename); \
	Write-Host ('Downloading {0} ...' -f $url); \
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; \
	Invoke-WebRequest -Uri $url -OutFile 'node.msi'; \
	\
	Write-Host 'Installing Node.js...'; \
	Start-Process msiexec.exe -Wait -ArgumentList '/I node.msi /quiet'; \
	\
#the installer updated PATH, so we should refresh our local value
	$env:PATH = [Environment]::GetEnvironmentVariable('PATH', [EnvironmentVariableTarget]::Machine); \
	\
	Write-Host 'Verifying install ...'; \
	Write-Host '  node --version'; node --version; \
	\
	Write-Host 'Removing setup file...'; \
	Remove-Item node.msi -Force; \
	\
	Write-Host 'Complete.'

#download and install python
RUN $url = ('https://www.python.org/ftp/python/{0}/python-{1}-amd64.exe' -f $env:PYTHON_RELEASE, $env:PYTHON_VERSION); \
	Write-Host ('Downloading {0} ...' -f $url); \
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; \
	Invoke-WebRequest -Uri $url -OutFile 'python.exe'; \
	\
	Write-Host 'Installing python...'; \
# https://docs.python.org/3.8/using/windows.html#installing-without-ui
	Start-Process python.exe -Wait \
		-ArgumentList @( \
			'/quiet', \
			'InstallAllUsers=1', \
			'TargetDir=C:\Python38', \
			'PrependPath=1', \
			'Shortcuts=0', \
			'Include_doc=0', \
			'Include_pip=1', \
			'Include_test=0' \
		); \
	\
#the installer updated PATH, so we should refresh our local value
	$env:PATH = [Environment]::GetEnvironmentVariable('PATH', [EnvironmentVariableTarget]::Machine); \
	\
	Write-Host 'Verifying install ...'; \
	Write-Host '  python --version'; python --version; \
	\
	Write-Host 'Upgrade pip ...'; \
	python -m pip install --upgrade pip; \
	\
	Write-Host 'Removing setup file...'; \
	Remove-Item python.exe -Force; \
	\
	Write-Host 'Complete.'


# we need to restart the wuauserv service. Otherwise the installation of the OpenSSH Server fails
# we also need to start and stop service sshd that all necessary config file are generated
RUN Write-Host 'Restart wuauserv service...'; \
	Set-Service -Name wuauserv -StartupType Manual; \
	Start-Service -name wuauserv; \
	Write-Host 'Install OpenSSH Server...'; \
	Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0; \
	Write-Host 'Initializing OpenSSH Server...'; \
	Start-Service sshd; \
	Stop-Service sshd


# create ciuser including key pair
RUN Write-Host 'Create user ciuser...';\
	NET USER ciuser /add
USER ciuser
RUN Write-Host 'Create key pair and copy public key...';\
	ssh-keygen -t rsa -N '\"\"' -f $env:USERPROFILE/.ssh/id_rsa; \
	cp $env:USERPROFILE/.ssh/id_rsa.pub $env:USERPROFILE/.ssh/authorized_keys

# switch back to the admin user 
USER containeradministrator

# this was the only way i found, to keep the sshd running 
# running it the context of a user (even if its the admin doesn't, back of some right issues)
CMD powershell -NoExit -Command "Start-Service sshd"


