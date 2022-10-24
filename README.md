## Шаги
- Создайте виртуальное окружение ```python3 -m venv env```;
- Запустите его ```source env/bin/activate```;
- Проверьте зависимости ```pip3 install -r requirements.txt```;

## Запуск
> ./test.py input/<tab> output/<tab>

## Багфиксы 
Если используете fish то используйте 
>source env/bin/activate

Если на сервере проблема с версиями Java, то
скачайте JDK-8 на [link](http://snapshot.debian.org/package/openjdk-8/8u322-b06-1%7Edeb9u1/)
Поставьте через dpkg -i и запустите 
>update-alternatives  --config java
