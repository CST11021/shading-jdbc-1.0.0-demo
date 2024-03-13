一、在github上创建工程
    步骤介绍：
    1、点击你的GitHub头像，选择下拉框中的“Settings”
    2、选择左侧菜单的“Developer settings”一项
    3、选择“Personal access tokens” 下的 Tokens(classic)
    4、点击“Generate new token(classic)”
    5、把以下选项全部勾选，然后点击下方绿色按钮，便可生成一个token
    详细步骤参见：https://www.jb51.net/article/195664.htm

二、执行如下命令将本地工程提交到远程仓库
git init
git add .
git commit -m "first commit"
git remote set-url origin https://${accessToken}@github.com/CST11021/${工程名称}.git
git push -u origin master


