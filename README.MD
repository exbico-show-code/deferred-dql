# Отложенное выполнение DQL

Тимлид: Пагинатор Pagerfanta в фоне выполняет запросы, некоторые не успевают в лимит по времени. 
Нужно отложить эти запросы в Редис для выполнения ночью. При этом нужно учесть, что популярным запросам нужно отдавать больший лимит (империческим путем выяснить)